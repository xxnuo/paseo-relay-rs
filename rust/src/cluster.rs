use super::Relay;
use crate::{config::node_address, session::ClaimEpoch};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, timeout},
};

const TTL: Duration = Duration::from_secs(3);
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_SNAPSHOT: usize = 16 * 1024 * 1024;
const MAX_NODES: usize = 1000;

type Nonce = [u8; 32];

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    version: u8,
    node: String,
    incarnation: [u8; 16],
    sequence: u64,
    target: String,
    claims: HashMap<String, ClaimEpoch>,
    members: Vec<String>,
}

struct Remote {
    incarnation: [u8; 16],
    retired: VecDeque<[u8; 16]>,
    sequence: u64,
    target: String,
    claims: HashMap<String, ClaimEpoch>,
    seen: Instant,
}

#[derive(Default)]
struct Membership {
    peers: HashMap<String, Remote>,
    known: HashMap<String, Instant>,
}

pub(super) struct Cluster {
    incarnation: [u8; 16],
    sequence: AtomicU64,
    membership: Mutex<Membership>,
    fault: AtomicBool,
}

impl Cluster {
    pub(super) fn new() -> Self {
        let mut incarnation = [0; 16];
        getrandom::fill(&mut incarnation).expect("cluster entropy unavailable");
        Self {
            incarnation,
            sequence: AtomicU64::new(1),
            membership: Mutex::new(Membership::default()),
            fault: AtomicBool::new(false),
        }
    }
}

enum Job {
    Outbound(String, io::Result<Snapshot>),
    Inbound(io::Result<Snapshot>),
    Discovery(Vec<std::net::SocketAddr>),
}

impl Relay {
    pub(super) fn cluster_size(&self) -> usize {
        1 + self.0.cluster.as_ref().map_or(0, |cluster| {
            cluster
                .membership
                .lock()
                .unwrap()
                .peers
                .values()
                .filter(|peer| peer.seen.elapsed() < TTL)
                .count()
        })
    }

    pub(super) fn remote_target(&self, id: &str) -> Option<String> {
        let sessions = self.0.sessions.lock().unwrap();
        self.remote_target_with_local(id, sessions.get(id).map(|session| session.claim))
    }

    pub(super) fn remote_target_with_local(
        &self,
        id: &str,
        local: Option<ClaimEpoch>,
    ) -> Option<String> {
        let cluster = self.0.cluster.as_ref()?;
        let config = self.0.config.cluster.as_ref()?;
        let state = cluster.membership.lock().unwrap();
        let mut best = local.map(|epoch| (epoch, config.node.as_str()));
        let mut target = None;
        for (node, peer) in &state.peers {
            if peer.seen.elapsed() >= TTL {
                continue;
            }
            if let Some(epoch) = peer.claims.get(id)
                && best.is_none_or(|winner| (*epoch, node.as_str()) < winner)
            {
                best = Some((*epoch, node.as_str()));
                target = Some(peer.target.clone());
            }
        }
        target
    }

    fn snapshot(&self) -> io::Result<Arc<Vec<u8>>> {
        let cluster = self.0.cluster.as_ref().unwrap();
        let config = self.0.config.cluster.as_ref().unwrap();
        let claims = self
            .0
            .sessions
            .lock()
            .unwrap()
            .iter()
            .map(|(id, session)| (id.clone(), session.claim))
            .collect();
        let members = cluster
            .membership
            .lock()
            .unwrap()
            .peers
            .iter()
            .filter(|(_, peer)| peer.seen.elapsed() < TTL)
            .map(|(node, _)| node.clone())
            .collect();
        let body = serde_json::to_vec(&Snapshot {
            version: 1,
            node: config.node.clone(),
            incarnation: cluster.incarnation,
            sequence: cluster.sequence.fetch_add(1, Ordering::Relaxed),
            target: self.0.config.ownership_target.clone(),
            claims,
            members,
        })?;
        if body.len() > MAX_SNAPSHOT {
            return Err(io::Error::other("cluster snapshot exceeds 16 MiB"));
        }
        Ok(Arc::new(body))
    }

    fn accept_snapshot(&self, snapshot: Snapshot) -> io::Result<()> {
        let config = self.0.config.cluster.as_ref().unwrap();
        let cluster = self.0.cluster.as_ref().unwrap();
        if snapshot.version != 1
            || snapshot.members.len() > MAX_NODES
            || snapshot.target.is_empty()
            || snapshot.target.len() > 1024
            || axum::http::HeaderValue::from_str(&snapshot.target).is_err()
            || snapshot
                .claims
                .keys()
                .any(|id| id.is_empty() || id.len() > 256)
        {
            return Err(invalid("invalid cluster snapshot"));
        }
        node_address(&snapshot.node).map_err(invalid)?;
        for member in &snapshot.members {
            node_address(member).map_err(invalid)?;
        }
        if snapshot.node == config.node {
            if snapshot.incarnation != cluster.incarnation {
                cluster.fault.store(true, Ordering::Relaxed);
            }
            return Ok(());
        }
        {
            let mut state = cluster.membership.lock().unwrap();
            state
                .peers
                .retain(|_, peer| peer.seen.elapsed() < Duration::from_secs(60));
            state
                .known
                .retain(|_, seen| seen.elapsed() < Duration::from_secs(60));
            if !state.peers.contains_key(&snapshot.node) && state.peers.len() >= MAX_NODES {
                return Err(invalid("cluster node limit"));
            }
            let mut retired = VecDeque::new();
            if let Some(previous) = state.peers.get_mut(&snapshot.node) {
                if previous.incarnation == snapshot.incarnation {
                    if previous.sequence >= snapshot.sequence {
                        if previous.sequence == snapshot.sequence {
                            previous.seen = Instant::now();
                        }
                        return Ok(());
                    }
                } else {
                    if previous.retired.contains(&snapshot.incarnation) {
                        return Ok(());
                    }
                    previous.retired.push_back(previous.incarnation);
                    if previous.retired.len() > 32 {
                        previous.retired.pop_front();
                    }
                }
                retired = std::mem::take(&mut previous.retired);
            }
            for node in snapshot
                .members
                .iter()
                .chain(std::iter::once(&snapshot.node))
            {
                if node != &config.node
                    && (state.known.contains_key(node) || state.known.len() < MAX_NODES)
                {
                    state.known.insert(node.clone(), Instant::now());
                }
            }
            state.peers.insert(
                snapshot.node,
                Remote {
                    incarnation: snapshot.incarnation,
                    retired,
                    sequence: snapshot.sequence,
                    target: snapshot.target,
                    claims: snapshot.claims,
                    seen: Instant::now(),
                },
            );
        }
        let mut sessions = self.0.sessions.lock().unwrap();
        sessions.retain(|id, session| {
            if self
                .remote_target_with_local(id, Some(session.claim))
                .is_some()
            {
                session.close_all();
                false
            } else {
                true
            }
        });
        Ok(())
    }

    pub(super) async fn run_cluster(&self) -> io::Result<()> {
        let Some(config) = self.0.config.cluster.as_ref() else {
            return std::future::pending().await;
        };
        let cluster = self.0.cluster.as_ref().unwrap();
        let listener = TcpListener::bind(config.listen).await?;
        let incoming = Arc::new(Semaphore::new(128));
        let staging = Arc::new(Semaphore::new(64 * 1024 * 1024));
        let mut jobs = JoinSet::new();
        let mut inflight = HashSet::new();
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut cached = self.snapshot()?;
        let mut dns: Vec<std::net::SocketAddr> = Vec::new();
        let mut discovery_running = false;
        let mut next_discovery = Instant::now();
        loop {
            tokio::select! {
                _=interval.tick()=>{
                    if cluster.fault.load(Ordering::Relaxed) { return Err(invalid("duplicate RELEASE_NODE identity")); }
                    {
                        let mut state=cluster.membership.lock().unwrap();
                        state.peers.retain(|_,peer|peer.seen.elapsed()<TTL);
                        state.known.retain(|_,seen|seen.elapsed()<Duration::from_secs(60));
                    }
                    cached=self.snapshot()?;
                    if !discovery_running && Instant::now()>=next_discovery && let Some(query)=&config.query {
                        discovery_running=true;
                        next_discovery=Instant::now()+Duration::from_secs(5);
                        let address=node_address(&format!("dns@{query}")).map_err(invalid)?;
                        jobs.spawn(async move {
                            let addresses=match timeout(RPC_TIMEOUT,tokio::net::lookup_host(address)).await {
                                Ok(Ok(addresses))=>addresses.take(MAX_NODES).collect(),_=>Vec::new(),
                            };
                            Job::Discovery(addresses)
                        });
                    }
                    let mut candidates=HashMap::new();
                    {
                        let state=cluster.membership.lock().unwrap();
                        for node in config.peers.iter().chain(state.known.keys()) {
                            if node!=&config.node { candidates.insert(node_address(node).map_err(invalid)?,Some(node.clone())); }
                        }
                    }
                    for address in &dns { candidates.entry(address.to_string()).or_insert(None); }
                    for (address,expected) in candidates {
                        if !inflight.insert(address.clone()) { continue; }
                        let cookie=config.cookie.clone(); let body=cached.clone(); let budget=staging.clone();
                        jobs.spawn(async move {
                            let result=timeout(RPC_TIMEOUT,async {
                                let mut stream=TcpStream::connect(&address).await?;
                                stream.set_nodelay(true)?;
                                let snapshot=client_exchange(&mut stream,&cookie,&body,&budget).await?;
                                if expected.is_some_and(|expected|expected!=snapshot.node) { return Err(invalid("cluster peer identity mismatch")); }
                                Ok(snapshot)
                            }).await.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"cluster RPC timeout")).and_then(|result|result);
                            Job::Outbound(address,result)
                        });
                    }
                },
                accepted=listener.accept()=>{
                    let (mut stream,_)=accepted?;
                    let Ok(permit)=incoming.clone().try_acquire_owned() else { continue; };
                    stream.set_nodelay(true)?;
                    let cookie=config.cookie.clone(); let body=cached.clone(); let budget=staging.clone();
                    jobs.spawn(async move {
                        let _permit=permit;
                        let result=timeout(RPC_TIMEOUT,server_exchange(&mut stream,&cookie,&body,&budget)).await.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"cluster RPC timeout")).and_then(|result|result);
                        Job::Inbound(result)
                    });
                },
                Some(job)=jobs.join_next(),if !jobs.is_empty()=>{
                    match job.map_err(io::Error::other)? {
                        Job::Outbound(address,result)=>{ inflight.remove(&address); if let Ok(snapshot)=result { let _=self.accept_snapshot(snapshot); } },
                        Job::Inbound(result)=>{ if let Ok(snapshot)=result { let _=self.accept_snapshot(snapshot); } },
                        Job::Discovery(addresses)=>{ discovery_running=false; dns=addresses; },
                    }
                },
            }
        }
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn authenticator(
    cookie: &[u8],
    domain: &[u8],
    server: &Nonce,
    client: &Nonce,
    payload: &[u8],
) -> Hmac<Sha256> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(cookie).expect("HMAC accepts arbitrary key lengths");
    mac.update(b"paseo-relay-cluster-v1\0");
    mac.update(domain);
    mac.update(server);
    mac.update(client);
    mac.update(payload);
    mac
}

fn nonce() -> io::Result<Nonce> {
    let mut nonce = [0; 32];
    getrandom::fill(&mut nonce).map_err(io::Error::other)?;
    Ok(nonce)
}

async fn client_exchange(
    stream: &mut TcpStream,
    cookie: &[u8],
    body: &[u8],
    budget: &Arc<Semaphore>,
) -> io::Result<Snapshot> {
    let mut server = [0; 32];
    stream.read_exact(&mut server).await?;
    let client = nonce()?;
    stream.write_all(&client).await?;
    let mut tag = [0; 32];
    stream.read_exact(&mut tag).await?;
    authenticator(cookie, b"hello", &server, &client, &[])
        .verify_slice(&tag)
        .map_err(|_| invalid("cluster authentication failed"))?;
    write_snapshot(stream, cookie, &server, &client, b"request", body).await?;
    read_snapshot(stream, cookie, &server, &client, b"response", budget).await
}

async fn server_exchange(
    stream: &mut TcpStream,
    cookie: &[u8],
    body: &[u8],
    budget: &Arc<Semaphore>,
) -> io::Result<Snapshot> {
    let server = nonce()?;
    stream.write_all(&server).await?;
    let mut client = [0; 32];
    stream.read_exact(&mut client).await?;
    stream
        .write_all(
            &authenticator(cookie, b"hello", &server, &client, &[])
                .finalize()
                .into_bytes(),
        )
        .await?;
    let snapshot = read_snapshot(stream, cookie, &server, &client, b"request", budget).await?;
    write_snapshot(stream, cookie, &server, &client, b"response", body).await?;
    Ok(snapshot)
}

async fn write_snapshot(
    stream: &mut TcpStream,
    cookie: &[u8],
    server: &Nonce,
    client: &Nonce,
    role: &[u8],
    body: &[u8],
) -> io::Result<()> {
    let length = (body.len() as u32).to_be_bytes();
    let header_domain = [role, b"-header"].concat();
    let body_domain = [role, b"-body"].concat();
    stream.write_all(&length).await?;
    stream
        .write_all(
            &authenticator(cookie, &header_domain, server, client, &length)
                .finalize()
                .into_bytes(),
        )
        .await?;
    stream.write_all(body).await?;
    stream
        .write_all(
            &authenticator(cookie, &body_domain, server, client, body)
                .finalize()
                .into_bytes(),
        )
        .await
}

async fn read_snapshot(
    stream: &mut TcpStream,
    cookie: &[u8],
    server: &Nonce,
    client: &Nonce,
    role: &[u8],
    budget: &Arc<Semaphore>,
) -> io::Result<Snapshot> {
    let length = stream.read_u32().await?;
    if length == 0 || length as usize > MAX_SNAPSHOT {
        return Err(invalid("cluster snapshot size"));
    }
    let mut tag = [0; 32];
    stream.read_exact(&mut tag).await?;
    let header_domain = [role, b"-header"].concat();
    let body_domain = [role, b"-body"].concat();
    authenticator(
        cookie,
        &header_domain,
        server,
        client,
        &length.to_be_bytes(),
    )
    .verify_slice(&tag)
    .map_err(|_| invalid("cluster authentication failed"))?;
    let _permit = budget
        .clone()
        .try_acquire_many_owned(length * 4)
        .map_err(|_| invalid("cluster staging capacity"))?;
    let mut body = vec![0; length as usize];
    stream.read_exact(&mut body).await?;
    stream.read_exact(&mut tag).await?;
    authenticator(cookie, &body_domain, server, client, &body)
        .verify_slice(&tag)
        .map_err(|_| invalid("cluster authentication failed"))?;
    serde_json::from_slice(&body).map_err(io::Error::other)
}
