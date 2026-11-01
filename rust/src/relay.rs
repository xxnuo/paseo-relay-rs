#[path = "cluster.rs"]
mod cluster;
#[path = "server.rs"]
mod server;

use crate::{
    Config,
    connection::{Connection, Role},
    protocol::{
        MAX_CONTROL_BYTES, MAX_MESSAGE_BYTES, control_ping, handshake, valid_query_encoding,
    },
    session::{Close, Delivery, Peer, Session, Write, payload},
};
use axum::{
    Extension, Router,
    extract::{
        Query, RawQuery, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{any, get},
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch},
    time::{Instant, timeout_at},
};

#[derive(Clone)]
pub struct Relay(Arc<Runtime>);

pub(crate) struct Pressure {
    pub watermark: usize,
    pub active: bool,
}

struct Runtime {
    config: Config,
    metrics: Arc<crate::metrics::Metrics>,
    cluster: Option<cluster::Cluster>,
    pressure: Mutex<Pressure>,
    draining: AtomicBool,
    slots: Arc<Semaphore>,
    ingress: Arc<Semaphore>,
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    next_id: AtomicU64,
}

impl Relay {
    pub fn new(config: Config) -> Self {
        assert!(config.max_connections > 0);
        assert!(config.ingress_weight > 0);
        let cluster = config.cluster.as_ref().map(|_| cluster::Cluster::new());
        Self(Arc::new(Runtime {
            cluster,
            metrics: Arc::new(crate::metrics::Metrics::default()),
            pressure: Mutex::new(Pressure {
                watermark: config.memory_watermark_bytes,
                active: false,
            }),
            draining: AtomicBool::new(config.drain),
            slots: Arc::new(Semaphore::new(config.max_connections)),
            ingress: Arc::new(Semaphore::new(config.ingress_budget_bytes)),
            config,
            sessions: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }))
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(
                "/health",
                any(|| async { ([("content-type", "application/json")], r#"{"status":"ok"}"#) }),
            )
            .route("/ready", any(ready))
            .route("/metrics", any(metrics))
            .route("/ws", get(upgrade))
            .fallback(|| async { (StatusCode::NOT_FOUND, "not found\n") })
            .with_state(self.clone())
    }

    pub fn begin_drain(&self) {
        self.0.draining.store(true, Ordering::Relaxed);
    }

    pub fn close_all(&self) {
        for session in self.0.sessions.lock().unwrap().values() {
            session.close_all();
        }
    }

    pub fn ingress_reserved_bytes(&self) -> usize {
        self.0.config.ingress_budget_bytes - self.0.ingress.available_permits()
    }

    pub fn active_connections(&self) -> usize {
        self.0.config.max_connections - self.0.slots.available_permits()
    }

    pub async fn wait_idle(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.active_connections() != 0
                || self.0.ingress.available_permits() != self.0.config.ingress_budget_bytes
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("relay did not return to idle");
    }

    fn ready(&self) -> bool {
        self.cluster_size() >= self.0.config.minimum_cluster_size
            && !self.0.draining.load(Ordering::Relaxed)
            && !self.0.pressure.lock().unwrap().active
            && self.0.slots.available_permits() > 0
    }
}

async fn ready(State(relay): State<Relay>) -> Response {
    let (status, body) = if relay.ready() {
        (StatusCode::OK, r#"{"status":"ready"}"#)
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, r#"{"status":"unready"}"#)
    };
    (status, [("content-type", "application/json")], body).into_response()
}

async fn metrics(State(relay): State<Relay>) -> Response {
    let mut body = String::new();
    for (name, value) in [
        ("ready", u64::from(relay.ready())),
        (
            "draining",
            u64::from(relay.0.draining.load(Ordering::Relaxed)),
        ),
        ("active_websockets", relay.active_connections() as u64),
        (
            "active_sessions",
            relay.0.sessions.lock().unwrap().len() as u64,
        ),
        ("cluster_nodes", relay.cluster_size() as u64),
        (
            "ingress_reserved_bytes",
            relay.ingress_reserved_bytes() as u64,
        ),
    ] {
        crate::metrics::scalar(&mut body, name, value);
    }
    relay.0.metrics.render(&mut body);
    if let Ok(rss) = server::resident_bytes() {
        crate::metrics::scalar(&mut body, "process_resident_memory_bytes", rss as u64);
    }
    ([("content-type", "text/plain; version=0.0.4")], body).into_response()
}

async fn upgrade(
    State(relay): State<Relay>,
    Query(query): Query<HashMap<String, String>>,
    RawQuery(raw): RawQuery,
    idle: Option<Extension<Arc<AtomicBool>>>,
    ws: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Response {
    let Ok(ws) = ws else {
        return (StatusCode::UPGRADE_REQUIRED, "Expected WebSocket upgrade").into_response();
    };
    if !valid_query_encoding(raw.as_deref().unwrap_or("")) {
        return (StatusCode::BAD_REQUEST, "Invalid query encoding").into_response();
    }
    let connection = match Connection::from_query(&query) {
        Ok(connection) => connection,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };
    if let Some(target) = relay.remote_target(&connection.server_id) {
        return reroute_response(&relay, target);
    }
    if relay.0.draining.load(Ordering::Relaxed) {
        return (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response();
    }
    if relay.0.pressure.lock().unwrap().active {
        relay.0.metrics.rejected.fetch_add(1, Ordering::Relaxed);
        return (StatusCode::SERVICE_UNAVAILABLE, "Relay memory pressure").into_response();
    }
    let Ok(slot) = relay.0.slots.clone().try_acquire_owned() else {
        relay.0.metrics.rejected.fetch_add(1, Ordering::Relaxed);
        return (StatusCode::SERVICE_UNAVAILABLE, "Relay connection capacity").into_response();
    };
    let lease = {
        let mut sessions = relay.0.sessions.lock().unwrap();
        if let Some(target) = relay.remote_target_with_local(
            &connection.server_id,
            sessions
                .get(&connection.server_id)
                .map(|session| session.claim),
        ) {
            return reroute_response(&relay, target);
        }
        if !sessions.contains_key(&connection.server_id)
            && relay.cluster_size() < relay.0.config.minimum_cluster_size
        {
            return (StatusCode::SERVICE_UNAVAILABLE, "cluster").into_response();
        }
        let session = sessions
            .entry(connection.server_id.clone())
            .or_default()
            .clone();
        if !session.reserve() {
            return (StatusCode::SERVICE_UNAVAILABLE, "owner").into_response();
        }
        Lease {
            relay: relay.clone(),
            session,
            server_id: connection.server_id.clone(),
            _slot: slot,
        }
    };
    let max = if is_control(&connection) {
        MAX_CONTROL_BYTES
    } else {
        MAX_MESSAGE_BYTES
    };
    if let Some(Extension(upgraded)) = idle {
        upgraded.store(true, Ordering::Relaxed);
    }
    ws.max_frame_size(max)
        .max_message_size(max)
        .write_buffer_size(0)
        .on_upgrade(move |socket| serve_socket(relay, connection, socket, lease))
}

fn is_control(connection: &Connection) -> bool {
    connection.version == 2
        && connection.role == Role::Server
        && connection.connection_id.as_deref() == Some("")
}

fn reroute_response(relay: &Relay, target: String) -> Response {
    relay.0.metrics.reroutes.fetch_add(1, Ordering::Relaxed);
    let mut response = (StatusCode::CONFLICT, "").into_response();
    response.headers_mut().insert(
        relay.0.config.reroute_header.clone(),
        axum::http::HeaderValue::from_str(&target).expect("validated target"),
    );
    response
}

struct Lease {
    relay: Relay,
    session: Arc<Session>,
    server_id: String,
    _slot: OwnedSemaphorePermit,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let Some(epoch) = self.session.release() else {
            return;
        };
        let relay = self.relay.clone();
        let session = self.session.clone();
        let id = self.server_id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let mut sessions = relay.0.sessions.lock().unwrap();
            if sessions
                .get(&id)
                .is_some_and(|current| Arc::ptr_eq(current, &session))
                && session.idle_at(epoch)
            {
                sessions.remove(&id);
            }
        });
    }
}

struct Attachment {
    session: Arc<Session>,
    peer: Arc<Peer>,
    _lease: Lease,
}

impl Drop for Attachment {
    fn drop(&mut self) {
        self.peer.close(1013, "Delivery unavailable");
        self.session.detach(&self.peer);
    }
}

async fn serve_socket(relay: Relay, connection: Connection, socket: WebSocket, lease: Lease) {
    let (sink, stream) = socket.split();
    let queue_capacity = if is_control(&connection) {
        relay.0.config.control_queue_bytes
    } else {
        1
    };
    let (tx, rx) = mpsc::channel(queue_capacity);
    let (close, closed) = watch::channel(None);
    let peer = Arc::new(Peer {
        id: relay.0.next_id.fetch_add(1, Ordering::Relaxed),
        metrics: relay.0.metrics.clone(),
        delivering: AtomicBool::new(false),
        connection,
        writes: tx,
        close,
        control_budget: Arc::new(Semaphore::new(relay.0.config.control_queue_bytes)),
        delivery_timeout: relay.0.config.delivery_timeout,
    });
    let session = lease.session.clone();
    let _attachment = Attachment {
        session: session.clone(),
        peer: peer.clone(),
        _lease: lease,
    };
    session.attach(peer.clone());
    let writer = writer(&relay, &peer, sink, rx, closed.clone());
    let reader = reader(&relay, &session, &peer, stream, closed);
    tokio::pin!(writer, reader);
    tokio::select! {
        _ = &mut writer => {},
        _ = &mut reader => {
            // Writer remains available for full-duplex traffic while source delivery is pending.
            peer.close(1000, "");
            let _ = timeout_at(Instant::now() + relay.0.config.delivery_timeout, &mut writer).await;
        }
    }
}

async fn writer(
    relay: &Relay,
    peer: &Peer,
    mut sink: SplitSink<WebSocket, Message>,
    mut writes: mpsc::Receiver<Write>,
    mut closed: watch::Receiver<Option<Close>>,
) {
    loop {
        let mut request = tokio::select! {
            biased;
            _ = closed.changed() => { break; },
            request = writes.recv() => match request { Some(request) => request, None => break },
        };
        if request.ack.as_ref().is_some_and(|ack| ack.is_closed()) {
            continue;
        }
        if Instant::now() >= request.deadline {
            peer.expire();
            break;
        }
        let Some(message) = request.message.upgrade() else {
            continue;
        };
        let len = payload(&message).len();
        // Match the reference emission counters; acknowledgements still enforce delivery.
        relay.0.metrics.frames.fetch_add(1, Ordering::Relaxed);
        relay
            .0
            .metrics
            .bytes
            .fetch_add(len as u64, Ordering::Relaxed);
        let write = timeout_at(request.deadline, sink.send((*message).clone()));
        let cancelled = async {
            if let Some(ack) = &mut request.ack {
                ack.closed().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        let ok = tokio::select! {
            biased;
            _ = closed.changed() => false,
            _ = cancelled => { peer.close(1013, "Delivery unavailable"); false },
            result = write => match result {
                Ok(Ok(())) => {
                    if Instant::now() <= request.deadline {true} else {peer.expire(); false}
                },
                Err(_) => { peer.expire(); false },
                _ => false,
            },
        };
        let abandoned = request.ack.is_some_and(|ack| ack.send(ok).is_err());
        if !ok || abandoned {
            break;
        }
    }
    let (code, reason) = closed.borrow().unwrap_or((1013, "Delivery unavailable"));
    let _ = timeout_at(
        Instant::now() + Duration::from_secs(1),
        sink.send(Message::Close(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))),
    )
    .await;
}

async fn reader(
    relay: &Relay,
    session: &Session,
    peer: &Peer,
    mut stream: SplitStream<WebSocket>,
    mut closed: watch::Receiver<Option<Close>>,
) {
    loop {
        let message = tokio::select! {
            biased;
            _ = closed.changed() => break,
            message = stream.next() => match message { Some(Ok(message)) => message,
                Some(Err(error)) => {
                    if error.into_inner().downcast_ref::<tokio_tungstenite::tungstenite::Error>().is_some_and(|error| matches!(error, tokio_tungstenite::tungstenite::Error::Capacity(_))) {
                        peer.close(1009, "Message too big");
                    } else {
                        peer.close(1002, "Invalid WebSocket frame");
                    }
                    break;
                },
                None => break },
        };
        if matches!(message, Message::Close(_)) {
            break;
        }
        if !matches!(message, Message::Text(_) | Message::Binary(_)) {
            continue;
        }
        if is_control(&peer.connection) && matches!(message, Message::Binary(_)) {
            continue;
        }
        relay.0.metrics.frame(payload(&message).len());
        let Some(weighted) = payload(&message)
            .len()
            .checked_mul(relay.0.config.ingress_weight)
            .and_then(|n| u32::try_from(n).ok())
        else {
            peer.close(1013, "Relay ingress capacity");
            break;
        };
        let Ok(_token) = relay.0.ingress.clone().try_acquire_many_owned(weighted) else {
            peer.close(1013, "Relay ingress capacity");
            break;
        };
        if is_control(&peer.connection) {
            if let Message::Text(text) = message
                && control_ping(text.as_bytes())
            {
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis();
                peer.notify(serde_json::json!({"type":"pong","ts":ts}));
            }
            continue;
        }
        if peer.connection.role == Role::Client
            && let Some(result) = handshake(payload(&message))
        {
            relay.0.metrics.handshake(peer.connection.version, result);
            if !result.accepted {
                peer.close(1008, "Invalid handshake key");
                break;
            }
        }
        let _delivery = Delivery::new(peer, payload(&message).len());
        let deadline = Instant::now() + relay.0.config.delivery_timeout;
        let delivery = async {
            let destinations = session
                .destinations(peer, &relay.0.config, deadline)
                .await?;
            if destinations.is_empty() {
                return Ok(());
            }
            let results = futures_util::future::join_all(
                destinations
                    .iter()
                    .map(|target| target.send(message.clone(), deadline)),
            )
            .await;
            if results.into_iter().any(|ok| ok) {
                Ok(())
            } else {
                Err((1013, "Delivery unavailable"))
            }
        };
        let result =
            tokio::select! { biased; _ = closed.changed() => break, result = delivery => result };
        if let Err((code, reason)) = result {
            peer.close(code, reason);
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn expired_control_is_closed_before_a_ready_transport_can_emit_it() {
        let relay = Relay::new(Config::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .route(
                "/writer",
                get(
                    |State(relay): State<Relay>, ws: WebSocketUpgrade| async move {
                        ws.on_upgrade(move |socket| async move {
                            let (sink, _reader) = socket.split();
                            let (writes, rx) = mpsc::channel(1);
                            let (close, closed) = watch::channel(None);
                            let peer = Peer {
                                id: 1,
                                metrics: relay.0.metrics.clone(),
                                delivering: AtomicBool::new(false),
                                connection: Connection::from_query(&HashMap::from([
                                    ("role".into(), "server".into()),
                                    ("serverId".into(), "expired-control".into()),
                                ]))
                                .unwrap(),
                                writes,
                                close,
                                control_budget: Arc::new(Semaphore::new(1024)),
                                delivery_timeout: Duration::from_secs(30),
                            };
                            let message = Arc::new(Message::Text("expired control".into()));
                            peer.writes
                                .try_send(Write {
                                    message: Arc::downgrade(&message),
                                    deadline: Instant::now() - Duration::from_millis(1),
                                    ack: None,
                                    _control: None,
                                })
                                .unwrap();
                            writer(&relay, &peer, sink, rx, closed).await;
                        })
                    },
                ),
            )
            .with_state(relay.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let (mut client, _) = tokio_tungstenite::connect_async(format!("ws://{address}/writer"))
            .await
            .unwrap();
        let message = tokio::time::timeout(Duration::from_secs(2), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let tokio_tungstenite::tungstenite::Message::Close(Some(close)) = message else {
            panic!("expired notification was emitted instead of closing");
        };
        assert_eq!(u16::from(close.code), 1013);
        assert_eq!(relay.0.metrics.frames.load(Ordering::Relaxed), 0);
        assert_eq!(relay.0.metrics.slow.load(Ordering::Relaxed), 1);
        assert_eq!(relay.0.metrics.timeouts.load(Ordering::Relaxed), 1);
        task.abort();
    }
}
