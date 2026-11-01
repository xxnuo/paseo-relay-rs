use crate::{
    Config,
    connection::{Connection, Role},
};
use axum::extract::ws::Message;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    time::Instant,
};

pub type Close = (u16, &'static str);

pub struct Write {
    pub message: Weak<Message>,
    pub deadline: Instant,
    pub ack: Option<oneshot::Sender<bool>>,
    pub _control: Option<(Arc<Message>, OwnedSemaphorePermit)>,
}

pub struct Peer {
    pub id: u64,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub delivering: AtomicBool,
    pub connection: Connection,
    pub writes: mpsc::Sender<Write>,
    pub close: watch::Sender<Option<Close>>,
    pub control_budget: Arc<Semaphore>,
    pub delivery_timeout: Duration,
}

impl Peer {
    pub fn close(&self, code: u16, reason: &'static str) -> bool {
        let changed = self.close.send_if_modified(|value| {
            if value.is_some() {
                false
            } else {
                *value = Some((code, reason));
                true
            }
        });
        if changed && code == 1013 {
            match reason {
                "Slow consumer" => {
                    self.metrics.slow.fetch_add(1, Ordering::Relaxed);
                }
                "Relay memory pressure" => {
                    self.metrics.pressure_closes.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
        changed
    }

    pub fn expire(&self) {
        if self.close(1013, "Slow consumer") {
            self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn notify(&self, value: serde_json::Value) {
        let message = Arc::new(Message::Text(value.to_string().into()));
        let Ok(permit) = self
            .control_budget
            .clone()
            .try_acquire_many_owned(payload(&message).len() as u32)
        else {
            self.close(1013, "Slow consumer");
            return;
        };
        if self
            .writes
            .try_send(Write {
                message: Arc::downgrade(&message),
                deadline: Instant::now() + self.delivery_timeout,
                ack: None,
                _control: Some((message, permit)),
            })
            .is_err()
        {
            self.close(1013, "Delivery unavailable");
        }
    }

    pub async fn send(&self, message: Message, deadline: Instant) -> bool {
        if self.close.borrow().is_some() {
            return false;
        }
        let (tx, rx) = oneshot::channel();
        // Queued data must not outlive the charged source; only control owns a queue-byte lease.
        let message = Arc::new(message);
        let queued = tokio::time::timeout_at(
            deadline,
            self.writes.send(Write {
                message: Arc::downgrade(&message),
                deadline,
                ack: Some(tx),
                _control: None,
            }),
        )
        .await;
        match queued {
            Ok(Ok(())) => {}
            Err(_) => {
                self.expire();
                return false;
            }
            _ => return false,
        }
        let result = match tokio::time::timeout_at(deadline, rx).await {
            Ok(result) => result.unwrap_or(false),
            Err(_) => {
                self.expire();
                false
            }
        };
        drop(message);
        result
    }
}

#[derive(Default)]
struct Topology {
    peers: HashMap<u64, Arc<Peer>>,
    leases: usize,
    epoch: u64,
    retired: bool,
    v1_server: Option<u64>,
    v1_client: Option<u64>,
    control: Option<u64>,
    clients: HashMap<String, HashSet<u64>>,
    data: HashMap<String, u64>,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ClaimEpoch {
    created_ms: u64,
    nonce: [u8; 16],
}

pub struct Session {
    pub claim: ClaimEpoch,
    topology: Mutex<Topology>,
    changed: watch::Sender<()>,
}

impl Default for Session {
    fn default() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).expect("session entropy unavailable");
        Self {
            claim: ClaimEpoch {
                created_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
                nonce,
            },
            topology: Mutex::new(Topology::default()),
            changed: watch::channel(()).0,
        }
    }
}

impl Session {
    pub fn reserve(&self) -> bool {
        let mut state = self.topology.lock().unwrap();
        if state.retired {
            return false;
        }
        state.leases += 1;
        state.epoch += 1;
        true
    }

    pub fn release(&self) -> Option<u64> {
        let mut state = self.topology.lock().unwrap();
        state.leases -= 1;
        state.epoch += 1;
        (state.leases == 0 && state.peers.is_empty()).then_some(state.epoch)
    }

    pub fn idle_at(&self, epoch: u64) -> bool {
        let state = self.topology.lock().unwrap();
        state.epoch == epoch && state.leases == 0 && state.peers.is_empty()
    }

    pub fn attach(self: &Arc<Self>, peer: Arc<Peer>) {
        let mut state = self.topology.lock().unwrap();
        if state.retired {
            peer.close(1012, "Session owner moved");
            return;
        }
        let connection = &peer.connection;
        let id = connection.connection_id.as_deref().unwrap_or("");
        let old = if connection.version == 1 {
            if connection.role == Role::Server {
                state.v1_server.replace(peer.id)
            } else {
                state.v1_client.replace(peer.id)
            }
        } else if connection.role == Role::Server && id.is_empty() {
            peer.notify(serde_json::json!({"type":"sync","connectionIds":state.clients.keys().collect::<Vec<_>>()}));
            state.control.replace(peer.id)
        } else if connection.role == Role::Server {
            state.data.insert(id.to_owned(), peer.id)
        } else {
            state
                .clients
                .entry(id.to_owned())
                .or_default()
                .insert(peer.id);
            Self::notify_control(
                &state,
                serde_json::json!({"type":"connected","connectionId":id}),
            );
            let weak = Arc::downgrade(self);
            let connection_id = id.to_owned();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let Some(session) = weak.upgrade() else {
                    return;
                };
                if !session.nudge(&connection_id, false) {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
                session.nudge(&connection_id, true);
            });
            None
        };
        if let Some(old) = old.and_then(|old| state.peers.get(&old)) {
            old.close(1008, "Replaced by new connection");
        }
        state.peers.insert(peer.id, peer);
        self.changed.send_replace(());
    }

    fn nudge(&self, id: &str, reset: bool) -> bool {
        let state = self.topology.lock().unwrap();
        if !state.clients.contains_key(id) || state.data.contains_key(id) {
            return false;
        }
        if reset {
            if let Some(control) = state.control.and_then(|id| state.peers.get(&id)) {
                control.close(1011, "Control unresponsive");
            }
        } else {
            Self::notify_control(
                &state,
                serde_json::json!({"type":"sync","connectionIds":state.clients.keys().collect::<Vec<_>>()}),
            );
        }
        true
    }

    fn notify_control(state: &Topology, message: serde_json::Value) {
        if let Some(peer) = state.control.and_then(|id| state.peers.get(&id)) {
            peer.notify(message);
        }
    }

    pub fn detach(&self, peer: &Peer) {
        let mut state = self.topology.lock().unwrap();
        if state.peers.remove(&peer.id).is_none() {
            return;
        }
        let connection = &peer.connection;
        let id = connection.connection_id.as_deref().unwrap_or("");
        if connection.version == 1 {
            let slot = if connection.role == Role::Server {
                &mut state.v1_server
            } else {
                &mut state.v1_client
            };
            if *slot == Some(peer.id) {
                *slot = None;
            }
        } else if connection.role == Role::Client {
            if let Some(clients) = state.clients.get_mut(id) {
                clients.remove(&peer.id);
                if clients.is_empty() {
                    state.clients.remove(id);
                    if let Some(data) = state.data.get(id).and_then(|id| state.peers.get(id)) {
                        data.close(1001, "Client disconnected");
                    }
                    Self::notify_control(
                        &state,
                        serde_json::json!({"type":"disconnected","connectionId":id}),
                    );
                }
            }
        } else if id.is_empty() {
            if state.control == Some(peer.id) {
                state.control = None;
            }
        } else if state.data.get(id) == Some(&peer.id) {
            state.data.remove(id);
            if let Some(clients) = state.clients.get(id) {
                for client in clients.iter().filter_map(|id| state.peers.get(id)) {
                    client.close(1012, "Server disconnected");
                }
            }
        }
        self.changed.send_replace(());
    }

    pub fn peers(&self) -> Vec<Arc<Peer>> {
        self.topology
            .lock()
            .unwrap()
            .peers
            .values()
            .cloned()
            .collect()
    }

    pub fn close_all(&self) {
        let mut state = self.topology.lock().unwrap();
        state.retired = true;
        for peer in state.peers.values() {
            peer.close(1012, "Session owner moved");
        }
        self.changed.send_replace(());
    }

    pub async fn destinations(
        &self,
        peer: &Peer,
        config: &Config,
        deadline: Instant,
    ) -> Result<Vec<Arc<Peer>>, Close> {
        let mut changed = self.changed.subscribe();
        let deadline = deadline.min(Instant::now() + config.data_attach_timeout);
        loop {
            {
                let state = self.topology.lock().unwrap();
                if state.retired || !state.peers.contains_key(&peer.id) {
                    return Err((1013, "Delivery unavailable"));
                }
                let connection = &peer.connection;
                let id = connection.connection_id.as_deref().unwrap_or("");
                let targets = if connection.version == 1 {
                    Some(
                        if connection.role == Role::Server {
                            state.v1_client
                        } else {
                            state.v1_server
                        }
                        .into_iter()
                        .collect::<Vec<_>>(),
                    )
                } else if connection.role == Role::Client {
                    state.data.get(id).map(|id| vec![*id])
                } else {
                    Some(
                        state
                            .clients
                            .get(id)
                            .map(|set| set.iter().copied().collect())
                            .unwrap_or_default(),
                    )
                };
                if let Some(targets) = targets {
                    return Ok(targets
                        .into_iter()
                        .filter_map(|id| state.peers.get(&id).cloned())
                        .collect());
                }
            }
            if tokio::time::timeout_at(deadline, changed.changed())
                .await
                .is_err()
            {
                return Err((1013, "Data route unavailable"));
            }
        }
    }
}

pub fn payload(message: &Message) -> &[u8] {
    match message {
        Message::Text(text) => text.as_bytes(),
        Message::Binary(bytes) | Message::Ping(bytes) | Message::Pong(bytes) => bytes,
        Message::Close(_) => &[],
    }
}

pub struct Delivery<'a> {
    peer: &'a Peer,
    bytes: usize,
    started: Instant,
}
impl<'a> Delivery<'a> {
    pub fn new(peer: &'a Peer, bytes: usize) -> Self {
        peer.delivering.store(true, Ordering::Relaxed);
        peer.metrics
            .inflight
            .fetch_add(bytes as u64, Ordering::Relaxed);
        peer.metrics.backpressured.fetch_add(1, Ordering::Relaxed);
        Self {
            peer,
            bytes,
            started: Instant::now(),
        }
    }
}
impl Drop for Delivery<'_> {
    fn drop(&mut self) {
        self.peer.delivering.store(false, Ordering::Relaxed);
        self.peer
            .metrics
            .inflight
            .fetch_sub(self.bytes as u64, Ordering::Relaxed);
        self.peer
            .metrics
            .backpressured
            .fetch_sub(1, Ordering::Relaxed);
        self.peer.metrics.delivery(self.started.elapsed());
    }
}
