use futures_util::{SinkExt, StreamExt};
use std::{
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error, Message},
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const COOKIE: &str = "test-only-cluster-secret-32-bytes-long";

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct Node {
    cookie: &'static str,
    reserved: std::sync::Mutex<Option<[std::net::TcpListener; 2]>>,
    public: SocketAddr,
    control: SocketAddr,
    id: String,
    target: String,
}
impl Node {
    fn new(name: &str) -> Self {
        let a = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let b = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let public = a.local_addr().unwrap();
        let control = b.local_addr().unwrap();
        Self {
            cookie: COOKIE,
            reserved: std::sync::Mutex::new(Some([a, b])),
            public,
            control,
            id: format!("{name}@{control}"),
            target: format!("target-{name}"),
        }
    }
    async fn start(
        &self,
        peers: &[&Node],
        floor: usize,
        limit: usize,
        dns: Option<String>,
    ) -> Process {
        drop(self.reserved.lock().unwrap().take());
        let mut command = Command::new(env!("CARGO_BIN_EXE_paseo-relay"));
        command
            .env_clear()
            .env("TOKIO_WORKER_THREADS", "2")
            .env("PASEO_RELAY_PORT", self.public.port().to_string())
            .env("PASEO_RELAY_CLUSTER_LISTEN", self.control.to_string())
            .env(
                "PASEO_RELAY_CLUSTER_NODES",
                peers
                    .iter()
                    .map(|peer| peer.id.as_str())
                    .collect::<Vec<_>>()
                    .join(","),
            )
            .env("PASEO_RELAY_MIN_CLUSTER_SIZE", floor.to_string())
            .env("PASEO_RELAY_OWNERSHIP_TARGET", &self.target)
            .env("PASEO_RELAY_ACCEPTORS", "1")
            .env("PASEO_RELAY_CONNECTIONS_PER_ACCEPTOR", limit.to_string())
            .env("RELEASE_NODE", &self.id)
            .env("RELEASE_COOKIE", self.cookie)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        if let Some(query) = dns {
            command.env("PASEO_RELAY_CLUSTER_QUERY", query);
        }
        let mut process = Process(command.spawn().unwrap());
        timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    process.0.try_wait().unwrap().is_none(),
                    "node exited before listening"
                );
                if TcpStream::connect(self.public).await.is_ok()
                    && TcpStream::connect(self.control).await.is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        process
    }
    fn url(&self, room: &str, role: &str) -> String {
        format!("ws://{}/ws?serverId={room}&role={role}", self.public)
    }
}

async fn request(node: &Node, path: &str) -> String {
    let mut stream = TcpStream::connect(node.public).await.unwrap();
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}
async fn ready(node: &Node) {
    timeout(Duration::from_secs(8), async {
        loop {
            if request(node, "/ready").await.starts_with("HTTP/1.1 200") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("cluster never became ready");
}
async fn receive(socket: &mut Socket) -> Message {
    timeout(Duration::from_secs(8), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn reroute(node: &Node, room: &str, target: &str) {
    timeout(Duration::from_secs(8), async {
        loop {
            match connect_async(node.url(room, "client")).await {
                Err(Error::Http(response)) if response.status() == 409 => {
                    assert_eq!(response.headers()["x-reroute-target"], target);
                    break;
                }
                Ok((mut temporary, _)) => {
                    temporary.close(None).await.unwrap();
                }
                other => panic!("unexpected routing result: {other:?}"),
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("remote ownership did not converge");
}

#[tokio::test]
async fn floor_blocks_unowned_admission_and_remote_reroute_bypasses_a_full_landing_node() {
    let a = Node::new("floor-a");
    let b = Node::new("floor-b");
    let _a = a.start(&[&b], 2, 1, None).await;
    assert!(request(&a, "/ready").await.starts_with("HTTP/1.1 503"));
    let Err(Error::Http(rejected)) = connect_async(a.url("rejected", "server")).await else {
        panic!("unowned admission should fail");
    };
    assert_eq!(rejected.status(), 503);
    assert!(
        request(&a, "/metrics")
            .await
            .contains("paseo_relay_active_sessions 0\n")
    );
    let _b = b.start(&[&a], 2, 100, None).await;
    ready(&a).await;
    ready(&b).await;
    let (mut local, _) = connect_async(a.url("local", "server")).await.unwrap();
    local.send(Message::Ping(vec![1].into())).await.unwrap();
    receive(&mut local).await;
    let (mut remote, _) = connect_async(b.url("remote", "server")).await.unwrap();
    remote.send(Message::Ping(vec![2].into())).await.unwrap();
    receive(&mut remote).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let Err(Error::Http(response)) = connect_async(a.url("remote", "client")).await else {
        panic!("expected reroute at capacity");
    };
    assert_eq!(response.status(), 409);
    assert_eq!(response.headers()["x-reroute-target"], b.target);
    local.close(None).await.unwrap();
    remote.close(None).await.unwrap();
}

#[tokio::test]
async fn dns_discovery_reroutes_then_public_replay_exchanges_ordered_payloads() {
    let a = Node::new("dns-a");
    let b = Node::new("dns-b");
    let _b = b.start(&[], 1, 100, None).await;
    let _a = a
        .start(&[], 2, 100, Some(format!("localhost:{}", b.control.port())))
        .await;
    ready(&a).await;
    let (mut daemon, _) = connect_async(b.url("dns-room", "server")).await.unwrap();
    daemon.send(Message::Ping(vec![1].into())).await.unwrap();
    receive(&mut daemon).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    reroute(&a, "dns-room", &b.target).await;
    let (mut client, _) = connect_async(b.url("dns-room", "client")).await.unwrap();
    for index in 0..20 {
        let message = Message::binary(vec![index; 1024]);
        client.send(message.clone()).await.unwrap();
        assert_eq!(receive(&mut daemon).await, message);
        daemon.send(message.clone()).await.unwrap();
        assert_eq!(receive(&mut client).await, message);
    }
    client.close(None).await.unwrap();
    daemon.close(None).await.unwrap();
}

struct Gate {
    open: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn gate(node: &mut Node) -> Gate {
    let listener = TcpListener::bind("127.0.0.2:0").await.unwrap();
    node.id = format!(
        "{}@{}",
        node.id.split_once('@').unwrap().0,
        listener.local_addr().unwrap()
    );
    let destination = node.control;
    let open = Arc::new(AtomicBool::new(true));
    let enabled = open.clone();
    let task = tokio::spawn(async move {
        let mut streams = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let (mut inbound,_)=accepted.unwrap();
                    if !enabled.load(Ordering::Relaxed) { continue; }
                    streams.spawn(async move {
                        if let Ok(mut outbound)=TcpStream::connect(destination).await { let _=tokio::io::copy_bidirectional(&mut inbound,&mut outbound).await; }
                    });
                },
                Some(_)=streams.join_next(),if !streams.is_empty()=>{},
            }
        }
    });
    Gate { open, task }
}

#[tokio::test]
async fn partition_heals_to_one_owner_and_loser_receives_explicit_retryable_close() {
    let mut a = Node::new("partition-a");
    let mut b = Node::new("partition-b");
    let ga = gate(&mut a).await;
    let gb = gate(&mut b).await;
    let _a = a.start(&[&b], 1, 100, None).await;
    let _b = b.start(&[&a], 1, 100, None).await;
    let (mut original, _) = connect_async(format!("{}&v=2", a.url("split", "server")))
        .await
        .unwrap();
    receive(&mut original).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    ga.open.store(false, Ordering::Relaxed);
    gb.open.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (mut duplicate, _) = connect_async(format!("{}&v=2", b.url("split", "server")))
        .await
        .unwrap();
    receive(&mut duplicate).await;
    ga.open.store(true, Ordering::Relaxed);
    gb.open.store(true, Ordering::Relaxed);
    let Message::Close(Some(close)) = receive(&mut duplicate).await else {
        panic!("losing owner did not close");
    };
    assert_eq!(u16::from(close.code), 1012);
    reroute(&b, "split", &a.target).await;
    original.send(Message::Ping(vec![3].into())).await.unwrap();
    assert_eq!(receive(&mut original).await, Message::Pong(vec![3].into()));
    original.close(None).await.unwrap();
}

#[tokio::test]
async fn dead_owner_expires_and_a_restarted_peer_learns_the_replacement_claim() {
    let a = Node::new("restart-a");
    let b = Node::new("restart-b");
    let pa = a.start(&[&b], 1, 100, None).await;
    let _b = b.start(&[&a], 1, 100, None).await;
    let (mut daemon, _) = connect_async(a.url("restart", "server")).await.unwrap();
    daemon.send(Message::Ping(vec![1].into())).await.unwrap();
    receive(&mut daemon).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    reroute(&b, "restart", &a.target).await;
    drop(pa);
    tokio::time::sleep(Duration::from_secs(4)).await;
    let (mut replacement, _) = connect_async(b.url("restart", "server")).await.unwrap();
    replacement
        .send(Message::Ping(vec![4].into()))
        .await
        .unwrap();
    receive(&mut replacement).await;
    let _new_a = a.start(&[&b], 1, 100, None).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    reroute(&a, "restart", &b.target).await;
    replacement.close(None).await.unwrap();
}

#[tokio::test]
async fn unauthenticated_cluster_input_cannot_raise_readiness_or_claim_ownership() {
    let a = Node::new("auth-a");
    let b = Node::new("auth-b");
    let _a = a.start(&[&b], 2, 100, None).await;
    let mut stream = TcpStream::connect(a.control).await.unwrap();
    let mut challenge = [0; 32];
    stream.read_exact(&mut challenge).await.unwrap();
    stream.write_all(&[0; 32]).await.unwrap();
    stream.write_u32(16 * 1024 * 1024).await.unwrap();
    stream.write_all(&[0; 32]).await.unwrap();
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), stream.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(request(&a, "/ready").await.starts_with("HTTP/1.1 503"));
    assert!(
        request(&a, "/metrics")
            .await
            .contains("paseo_relay_active_sessions 0\n")
    );
    assert!(request(&a, "/cluster").await.starts_with("HTTP/1.1 404"));
}

#[tokio::test]
async fn peers_learn_transitive_membership_and_converge_all_claims_across_three_processes() {
    let a = Node::new("mesh-a");
    let b = Node::new("mesh-b");
    let c = Node::new("mesh-c");
    let _a = a.start(&[&b], 3, 100, None).await;
    let _b = b.start(&[&c], 3, 100, None).await;
    let _c = c.start(&[], 3, 100, None).await;
    ready(&a).await;
    ready(&b).await;
    ready(&c).await;
    let nodes = [&a, &b, &c];
    let mut sockets = Vec::new();
    for index in 0..9 {
        let (mut socket, _) =
            connect_async(nodes[index % 3].url(&format!("mesh{index}"), "server"))
                .await
                .unwrap();
        socket
            .send(Message::Ping(vec![index as u8].into()))
            .await
            .unwrap();
        receive(&mut socket).await;
        sockets.push(socket);
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    for index in 0..9 {
        for node in nodes.iter().filter(|node| node.id != nodes[index % 3].id) {
            reroute(node, &format!("mesh{index}"), &nodes[index % 3].target).await;
        }
    }
    for node in nodes {
        assert!(
            request(node, "/metrics")
                .await
                .contains("paseo_relay_active_sessions 3\n")
        );
        assert!(
            request(node, "/metrics")
                .await
                .contains("paseo_relay_cluster_nodes 3\n")
        );
    }
    for socket in &mut sockets {
        socket.close(None).await.unwrap();
    }
}

#[tokio::test]
async fn different_cluster_cookies_never_count_each_other_toward_the_floor() {
    let a = Node::new("wrong-cookie-a");
    let mut b = Node::new("wrong-cookie-b");
    b.cookie = "different-test-cookie-at-least-32-bytes";
    let _a = a.start(&[&b], 2, 100, None).await;
    let _b = b.start(&[&a], 2, 100, None).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    for node in [&a, &b] {
        assert!(request(node, "/ready").await.starts_with("HTTP/1.1 503"));
        assert!(
            request(node, "/metrics")
                .await
                .contains("paseo_relay_cluster_nodes 1\n")
        );
    }
}

#[tokio::test]
async fn an_authenticated_snapshot_cannot_be_replayed_under_a_fresh_server_challenge() {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    fn sign(domain: &[u8], server: &[u8; 32], client: &[u8; 32], payload: &[u8]) -> Vec<u8> {
        let mut mac = Hmac::<Sha256>::new_from_slice(COOKIE.as_bytes()).unwrap();
        mac.update(b"paseo-relay-cluster-v1\0");
        mac.update(domain);
        mac.update(server);
        mac.update(client);
        mac.update(payload);
        mac.finalize().into_bytes().to_vec()
    }
    let a = Node::new("replay-a");
    let b = Node::new("replay-b");
    let _a = a.start(&[&b], 2, 100, None).await;
    let mut first = TcpStream::connect(a.control).await.unwrap();
    let mut original_challenge = [0; 32];
    first.read_exact(&mut original_challenge).await.unwrap();
    let client_nonce = [11; 32];
    first.write_all(&client_nonce).await.unwrap();
    let mut hello = [0; 32];
    first.read_exact(&mut hello).await.unwrap();
    assert_eq!(
        hello.as_slice(),
        sign(b"hello", &original_challenge, &client_nonce, &[])
    );
    let body=serde_json::to_vec(&serde_json::json!({"version":1,"node":b.id,"incarnation":vec![3u8;16],"sequence":1,"target":b.target,"claims":{},"members":[]})).unwrap();
    let length = (body.len() as u32).to_be_bytes();
    let mut packet = length.to_vec();
    packet.extend_from_slice(&sign(
        b"request-header",
        &original_challenge,
        &client_nonce,
        &length,
    ));
    packet.extend_from_slice(&body);
    packet.extend_from_slice(&sign(
        b"request-body",
        &original_challenge,
        &client_nonce,
        &body,
    ));
    first.write_all(&packet).await.unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(3), first.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    ready(&a).await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(request(&a, "/ready").await.starts_with("HTTP/1.1 503"));
    let mut replay = TcpStream::connect(a.control).await.unwrap();
    let mut fresh_challenge = [0; 32];
    replay.read_exact(&mut fresh_challenge).await.unwrap();
    assert_ne!(original_challenge, fresh_challenge);
    replay.write_all(&client_nonce).await.unwrap();
    replay.read_exact(&mut hello).await.unwrap();
    replay.write_all(&packet).await.unwrap();
    response.clear();
    let _ = timeout(Duration::from_secs(3), replay.read_to_end(&mut response))
        .await
        .unwrap();
    assert!(request(&a, "/ready").await.starts_with("HTTP/1.1 503"));
    assert!(
        request(&a, "/metrics")
            .await
            .contains("paseo_relay_cluster_nodes 1\n")
    );
}

#[tokio::test]
async fn duplicate_authenticated_node_identity_fails_closed_with_explicit_socket_closes() {
    let a = Node::new("duplicate-a");
    let mut b = Node::new("duplicate-b");
    let proxy = gate(&mut b).await;
    proxy.open.store(false, Ordering::Relaxed);
    let discovery = b.id.split_once('@').unwrap().1.to_owned();
    b.id = a.id.clone();
    let _b = b.start(&[], 1, 100, None).await;
    let _a = a.start(&[], 1, 100, Some(discovery)).await;
    let (mut owner, _) = connect_async(format!("{}&v=2", a.url("duplicate-identity", "server")))
        .await
        .unwrap();
    receive(&mut owner).await;
    proxy.open.store(true, Ordering::Relaxed);
    let Message::Close(Some(close)) = receive(&mut owner).await else {
        panic!("authority loss did not close the live socket");
    };
    assert_eq!(u16::from(close.code), 1012);
    assert!(
        connect_async(a.url("must-not-claim", "server"))
            .await
            .is_err()
    );
}
