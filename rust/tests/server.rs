use futures_util::{SinkExt, StreamExt};
use paseo_relay::{Config, Relay};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

async fn start(
    config: Config,
) -> (
    std::net::SocketAddr,
    Relay,
    tokio::task::JoinHandle<std::io::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let relay = Relay::new(config);
    let service = relay.clone();
    let task =
        tokio::spawn(async move { service.serve(listener, std::future::pending::<()>()).await });
    (addr, relay, task)
}

#[tokio::test]
async fn idle_http_and_withheld_request_body_expire_but_websockets_do_not() {
    let (addr, relay, task) = start(Config {
        http_idle_timeout: Duration::from_millis(100),
        ..Config::default()
    })
    .await;
    let mut partial = TcpStream::connect(addr).await.unwrap();
    partial
        .write_all(b"GET /health HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    let mut output = Vec::new();
    timeout(Duration::from_secs(2), partial.read_to_end(&mut output))
        .await
        .unwrap()
        .unwrap();
    let mut body = TcpStream::connect(addr).await.unwrap();
    body.write_all(b"POST /health HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\n")
        .await
        .unwrap();
    output.clear();
    timeout(Duration::from_secs(2), body.read_to_end(&mut output))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&output).starts_with("HTTP/1.1 200"));
    let (mut socket, _) = connect_async(format!("ws://{addr}/ws?serverId=idle&role=server"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(250)).await;
    socket
        .send(Message::Ping(vec![1, 2, 3].into()))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::Pong(vec![1, 2, 3].into())
    );
    socket.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn pressure_sheds_an_incomplete_fragment_and_admission_recovers_when_disabled() {
    let (addr, relay, task) = start(Config::default()).await;
    let (mut socket, _) = connect_async(format!("ws://{addr}/ws?serverId=pressure&role=server"))
        .await
        .unwrap();
    // Leave a fragmented message in the parser, outside the complete-message byte budget.
    use tokio_tungstenite::tungstenite::protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    };
    socket
        .send(Message::Frame(Frame::message(
            vec![7; 1024 * 1024],
            OpCode::Data(Data::Binary),
            false,
        )))
        .await
        .unwrap();
    assert_eq!(relay.ingress_reserved_bytes(), 0);
    relay.set_memory_watermark(1);
    let Message::Close(Some(frame)) = timeout(Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    else {
        panic!("missing pressure close")
    };
    assert_eq!(u16::from(frame.code), 1013);
    assert_eq!(frame.reason, "Relay memory pressure");
    relay.wait_idle().await;
    assert!(
        connect_async(format!("ws://{addr}/ws?serverId=rejected&role=server"))
            .await
            .is_err()
    );
    assert!(request(addr, "/ready").await.starts_with("HTTP/1.1 503"));
    relay.set_memory_watermark(0);
    assert!(request(addr, "/ready").await.starts_with("HTTP/1.1 200"));
    let (mut next, _) = connect_async(format!("ws://{addr}/ws?serverId=recovery&role=server"))
        .await
        .unwrap();
    next.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}

async fn request(addr: std::net::SocketAddr, path: &str) -> String {
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(2), socket.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn stale_idle_timer_cannot_remove_a_reused_session_before_its_new_grace_period() {
    let (addr, relay, task) = start(Config::default()).await;
    for room in ["reused", "unused"] {
        let (mut socket, _) = connect_async(format!("ws://{addr}/ws?serverId={room}&role=server"))
            .await
            .unwrap();
        socket.send(Message::Ping(vec![1].into())).await.unwrap();
        socket.next().await.unwrap().unwrap();
        socket.close(None).await.unwrap();
        relay.wait_idle().await;
    }
    tokio::time::sleep(Duration::from_secs(28)).await;
    let (mut socket, _) = connect_async(format!("ws://{addr}/ws?serverId=reused&role=server"))
        .await
        .unwrap();
    socket.send(Message::Ping(vec![2].into())).await.unwrap();
    socket.next().await.unwrap().unwrap();
    socket.close(None).await.unwrap();
    relay.wait_idle().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        request(addr, "/metrics")
            .await
            .contains("paseo_relay_active_sessions 1\n"),
        "the previous idle timer must not expire a newer owner lease"
    );
    task.abort();
}
