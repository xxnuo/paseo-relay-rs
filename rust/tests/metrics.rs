use futures_util::{SinkExt, StreamExt};
use paseo_relay::{Config, Relay};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

async fn metrics(address: std::net::SocketAddr) -> String {
    let mut tcp = TcpStream::connect(address).await.unwrap();
    tcp.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), tcp.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}
fn value(text: &str, series: &str) -> f64 {
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("paseo_relay_{series} ")))
        .unwrap_or_else(|| panic!("missing {series}"))
        .parse()
        .unwrap()
}

#[tokio::test]
async fn prometheus_tracks_real_frames_handshakes_delivery_and_control_input() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let relay = Relay::new(Config::default());
    let service = relay.clone();
    let task = tokio::spawn(async move {
        service
            .serve(listener, std::future::pending::<()>())
            .await
            .unwrap();
    });
    let initial = metrics(address).await;
    for (name, kind) in [
        ("inflight_delivery_bytes", "gauge"),
        ("backpressured_sources", "gauge"),
        ("delivery_timeouts_total", "counter"),
        ("frame_size_bytes", "histogram"),
        ("delivery_wait_seconds", "histogram"),
        ("handshake_accepted_total", "counter"),
        ("handshake_rejected_total", "counter"),
    ] {
        assert!(
            initial.contains(&format!("# TYPE paseo_relay_{name} {kind}\n")),
            "missing metric metadata for {name}"
        );
    }
    let (mut daemon, _) = connect_async(format!("ws://{address}/ws?serverId=metrics&role=server"))
        .await
        .unwrap();
    let (mut client, _) = connect_async(format!("ws://{address}/ws?serverId=metrics&role=client"))
        .await
        .unwrap();
    let accepted = r#"{"type":"hello","key":"CQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}"#;
    client
        .send(Message::binary(accepted.as_bytes().to_vec()))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), daemon.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::binary(accepted.as_bytes().to_vec())
    );
    let rejected = r#"{"type":"e2ee_hello","key":"invalid"}"#;
    client.send(Message::text(rejected)).await.unwrap();
    let Message::Close(Some(close)) = timeout(Duration::from_secs(3), client.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    else {
        panic!("missing invalid-key close");
    };
    assert_eq!(u16::from(close.code), 1008);
    let observed = metrics(address).await;
    assert_eq!(
        value(
            &observed,
            "handshake_accepted_total{routing_version=\"v1\",type=\"hello\"}"
        ),
        1.0
    );
    assert_eq!(
        value(
            &observed,
            "handshake_rejected_total{routing_version=\"v1\",type=\"e2ee_hello\"}"
        ),
        1.0
    );
    assert_eq!(value(&observed, "frame_size_bytes_count"), 2.0);
    assert_eq!(
        value(&observed, "frame_size_bytes_sum"),
        (accepted.len() + rejected.len()) as f64
    );
    assert_eq!(
        value(&observed, "frame_size_bytes_bucket{le=\"1024\"}"),
        2.0
    );
    assert_eq!(value(&observed, "max_frame_bytes"), accepted.len() as f64);
    assert_eq!(value(&observed, "delivery_wait_seconds_count"), 1.0);
    assert_eq!(value(&observed, "inflight_delivery_bytes"), 0.0);
    assert_eq!(value(&observed, "backpressured_sources"), 0.0);
    let (mut control, _) = connect_async(format!(
        "ws://{address}/ws?serverId=control-metrics&role=server&v=2"
    ))
    .await
    .unwrap();
    control.next().await.unwrap().unwrap();
    control
        .send(Message::Binary(vec![1, 2, 3].into()))
        .await
        .unwrap();
    control
        .send(Message::text(r#"{"type":"ping"}"#))
        .await
        .unwrap();
    let pong = timeout(Duration::from_secs(3), control.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(pong.to_text().unwrap()).unwrap()["type"],
        "pong"
    );
    let observed = metrics(address).await;
    assert_eq!(
        value(&observed, "frame_size_bytes_count"),
        3.0,
        "ignored binary control frames do not enter the histogram"
    );
    assert_eq!(value(&observed, "delivery_wait_seconds_count"), 1.0);
    daemon.close(None).await.unwrap();
    control.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn waiting_for_attachment_exposes_and_releases_live_delivery_gauges() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let relay = Relay::new(Config {
        data_attach_timeout: Duration::from_millis(500),
        ..Config::default()
    });
    let service = relay.clone();
    let task = tokio::spawn(async move {
        service
            .serve(listener, std::future::pending::<()>())
            .await
            .unwrap();
    });
    let (mut client, _) = connect_async(format!(
        "ws://{address}/ws?serverId=waiting&role=client&v=2&connectionId=missing"
    ))
    .await
    .unwrap();
    client.send(Message::binary(vec![1; 1024])).await.unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let text = metrics(address).await;
            if value(&text, "inflight_delivery_bytes") == 1024.0 {
                assert_eq!(value(&text, "backpressured_sources"), 1.0);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.next().await.unwrap().unwrap();
    relay.wait_idle().await;
    let text = metrics(address).await;
    assert_eq!(value(&text, "inflight_delivery_bytes"), 0.0);
    assert_eq!(value(&text, "backpressured_sources"), 0.0);
    assert_eq!(value(&text, "delivery_wait_seconds_count"), 1.0);
    task.abort();
}

#[tokio::test]
async fn slow_consumer_timeouts_are_counted_once_and_control_budget_closes_are_distinct() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let relay = Relay::new(Config {
        delivery_timeout: Duration::from_millis(100),
        control_queue_bytes: 64,
        ..Config::default()
    });
    let service = relay.clone();
    let task = tokio::spawn(async move {
        service
            .serve(listener, std::future::pending::<()>())
            .await
            .unwrap();
    });
    let (mut unread, _) = connect_async(format!(
        "ws://{address}/ws?serverId=slow-metrics&role=server"
    ))
    .await
    .unwrap();
    unread.send(Message::Ping(vec![1].into())).await.unwrap();
    timeout(Duration::from_secs(3), unread.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let (mut source, _) = connect_async(format!(
        "ws://{address}/ws?serverId=slow-metrics&role=client"
    ))
    .await
    .unwrap();
    source
        .send(Message::binary(vec![7; 8 * 1024 * 1024]))
        .await
        .unwrap();
    let Message::Close(Some(close)) = timeout(Duration::from_secs(3), source.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    else {
        panic!("missing source close");
    };
    assert_eq!(u16::from(close.code), 1013);
    relay.wait_idle().await;
    let before = metrics(address).await;
    assert_eq!(value(&before, "slow_consumer_disconnects_total"), 1.0);
    assert_eq!(value(&before, "delivery_timeouts_total"), 1.0);
    assert_eq!(
        value(&before, "frames_forwarded_total"),
        1.0,
        "reference counters count Writer emission even when its barrier later times out"
    );
    let mut clients = Vec::new();
    for letter in ["a", "b", "c"] {
        let (mut socket, _) = connect_async(format!(
            "ws://{address}/ws?serverId=queue-metrics&role=client&v=2&connectionId={}",
            letter.repeat(32)
        ))
        .await
        .unwrap();
        socket.send(Message::Ping(vec![1].into())).await.unwrap();
        timeout(Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        clients.push(socket);
    }
    let (mut control, _) = connect_async(format!(
        "ws://{address}/ws?serverId=queue-metrics&role=server&v=2"
    ))
    .await
    .unwrap();
    let Message::Close(Some(close)) = timeout(Duration::from_secs(3), control.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    else {
        panic!("oversized sync should close its bounded control destination");
    };
    assert_eq!(u16::from(close.code), 1013);
    let after = metrics(address).await;
    assert_eq!(value(&after, "slow_consumer_disconnects_total"), 2.0);
    assert_eq!(
        value(&after, "delivery_timeouts_total"),
        1.0,
        "queue-byte rejection is not a delivery timeout"
    );
    for socket in &mut clients {
        socket.close(None).await.unwrap();
    }
    relay.wait_idle().await;
    task.abort();
}
