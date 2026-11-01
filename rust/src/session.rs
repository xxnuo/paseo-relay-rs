use crate::{
    Config,
    connection::{Connection, Role},
};
use axum::extract::ws::Message;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    time::Instant,
};

pub type Close = (u16, &'static str);

pub struct Write {
    pub message: Message,
    pub deadline: Instant,
    pub ack: Option<oneshot::Sender<bool>>,
    pub _control: Option<OwnedSemaphorePermit>,
}

pub struct Peer {
    pub id: u64,
    pub delivering: AtomicBool,
    pub connection: Connection,
    pub writes: mpsc::UnboundedSender<Write>,
    pub close: watch::Sender<Option<Close>>,
    pub control_budget: Arc<Semaphore>,
    pub delivery_timeout: Duration,
}

impl Peer {
    pub fn close(&self, code: u16, reason: &'static str) {
        self.close.send_if_modified(|value| {
            if value.is_some() {
                false
            } else {
                *value = Some((code, reason));
                true
            }
        });
    }

    pub fn notify(&self, value: serde_json::Value) {
        let message = Message::Text(value.to_string().into());
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
            .send(Write {
                message,
                deadline: Instant::now() + self.delivery_timeout,
                ack: None,
                _control: Some(permit),
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
        if self
            .writes
            .send(Write {
                message,
                deadline,
                ack: Some(tx),
                _control: None,
            })
            .is_err()
        {
            return false;
        }
        // Expiry closes the entire destination, including any write already emitted.
        match tokio::time::timeout_at(deadline, rx).await {
            Ok(result) => result.unwrap_or(false),
            Err(_) => {
                self.close(1013, "Slow consumer");
                false
            }
        }
    }
}

#[derive(Default)]
struct Topology {
    peers: HashMap<u64, Arc<Peer>>,
    v1_server: Option<u64>,
    v1_client: Option<u64>,
    control: Option<u64>,
    clients: HashMap<String, HashSet<u64>>,
    data: HashMap<String, u64>,
}

pub struct Session {
    topology: Mutex<Topology>,
    changed: watch::Sender<()>,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            topology: Mutex::new(Topology::default()),
            changed: watch::channel(()).0,
        }
    }
}

impl Session {
    pub fn attach(self: &Arc<Self>, peer: Arc<Peer>) {
        let mut state = self.topology.lock().unwrap();
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
        state.peers.remove(&peer.id);
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
        for peer in self.topology.lock().unwrap().peers.values() {
            peer.close(1012, "Session owner moved");
        }
    }

    pub fn is_empty(&self) -> bool {
        self.topology.lock().unwrap().peers.is_empty()
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
                if !state.peers.contains_key(&peer.id) {
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

pub struct Delivery<'a>(pub &'a Peer);
impl Drop for Delivery<'_> {
    fn drop(&mut self) {
        self.0.delivering.store(false, Ordering::Relaxed);
    }
}
