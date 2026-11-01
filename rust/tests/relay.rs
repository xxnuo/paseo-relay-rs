use futures_util::{SinkExt, StreamExt};
use paseo_relay::{Config, Relay};
use tokio::{
    net::{TcpListener, TcpStream},
    time::{Duration, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, connect_async_with_config, tungstenite::Message,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start(config: Config) -> (String, Relay, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let relay = Relay::new(config);
    let service = relay.clone();
    let task = tokio::spawn(async move {
        service
            .serve(listener, std::future::pending::<()>())
            .await
            .unwrap();
    });
    (format!("ws://{addr}/ws?serverId=test"), relay, task)
}

async fn connect(url: &str, query: &str) -> Socket {
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_frame_size(Some(paseo_relay::protocol::MAX_MESSAGE_BYTES));
    connect_async_with_config(format!("{url}&{query}"), Some(config), true)
        .await
        .unwrap()
        .0
}

async fn receive(socket: &mut Socket) -> Message {
    timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn json(socket: &mut Socket) -> serde_json::Value {
    serde_json::from_str(receive(socket).await.to_text().unwrap()).unwrap()
}

async fn closed(socket: &mut Socket, code: u16) {
    let Message::Close(Some(frame)) = receive(socket).await else {
        panic!("expected explicit close");
    };
    assert_eq!(u16::from(frame.code), code);
}

#[tokio::test]
async fn v1_forwards_both_opcodes_and_replaces_only_same_role() {
    let (url, relay, task) = start(Config::default()).await;
    let mut daemon = connect(&url, "role=server").await;
    let mut client = connect(&url, "role=client").await;
    for msg in [
        Message::text("hello opaque"),
        Message::binary(vec![0xff; 64]),
    ] {
        client.send(msg.clone()).await.unwrap();
        assert_eq!(receive(&mut daemon).await, msg);
        daemon.send(msg.clone()).await.unwrap();
        assert_eq!(receive(&mut client).await, msg);
    }
    let mut replacement = connect(&url, "role=server").await;
    closed(&mut daemon, 1008).await;
    client.send(Message::text("replacement")).await.unwrap();
    assert_eq!(
        receive(&mut replacement).await,
        Message::text("replacement")
    );
    client.close(None).await.unwrap();
    replacement.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn v2_notifies_waits_for_data_and_preserves_last_messages_before_close() {
    let (url, relay, task) = start(Config::default()).await;
    let mut control = connect(&url, "role=server&v=2").await;
    assert_eq!(
        json(&mut control).await,
        serde_json::json!({"type":"sync","connectionIds":[]})
    );
    let mut client = connect(&url, "role=client&v=2&connectionId=peer").await;
    assert_eq!(
        json(&mut control).await,
        serde_json::json!({"type":"connected","connectionId":"peer"})
    );
    client.send(Message::text("before attach")).await.unwrap();
    let mut data = connect(&url, "role=server&v=2&connectionId=peer").await;
    assert_eq!(receive(&mut data).await, Message::text("before attach"));
    for i in 0..3 {
        client
            .send(Message::text(format!("last {i}")))
            .await
            .unwrap();
    }
    client.close(None).await.unwrap();
    for i in 0..3 {
        assert_eq!(receive(&mut data).await, Message::text(format!("last {i}")));
    }
    closed(&mut data, 1001).await;
    assert_eq!(
        json(&mut control).await,
        serde_json::json!({"type":"disconnected","connectionId":"peer"})
    );
    control.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn validates_client_handshakes_over_real_websockets() {
    let (url, relay, task) = start(Config::default()).await;
    let mut daemon = connect(&url, "role=server").await;
    let mut client = connect(&url, "role=client").await;
    client
        .send(Message::text(r#"{"type":"hello","key":"invalid"}"#))
        .await
        .unwrap();
    closed(&mut client, 1008).await;
    assert!(
        timeout(Duration::from_millis(50), daemon.next())
            .await
            .is_err()
    );
    daemon.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn capacity_rejection_and_attach_deadline_recover_without_leaking_slots() {
    let config = Config {
        max_connections: 1,
        data_attach_timeout: Duration::from_millis(100),
        ..Config::default()
    };
    let (url, relay, task) = start(config).await;
    let mut client = connect(&url, "role=client&v=2&connectionId=missing").await;
    let error = connect_async(format!("{url}&role=server"))
        .await
        .unwrap_err();
    let tokio_tungstenite::tungstenite::Error::Http(response) = error else {
        panic!("expected HTTP rejection")
    };
    assert_eq!(response.status(), 503);
    client.send(Message::text("wait")).await.unwrap();
    closed(&mut client, 1013).await;
    relay.wait_idle().await;
    let mut next = connect(&url, "role=server").await;
    next.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn v2_fanout_and_replacement_keep_client_routes_and_notify_control() {
    let (url, relay, task) = start(Config::default()).await;
    let mut control = connect(&url, "role=server&v=2").await;
    json(&mut control).await;
    let mut a = connect(&url, "role=client&v=2&connectionId=shared").await;
    json(&mut control).await;
    let mut b = connect(&url, "role=client&v=2&connectionId=shared").await;
    json(&mut control).await;
    let mut old = connect(&url, "role=server&v=2&connectionId=shared").await;
    old.send(Message::text("attached")).await.unwrap();
    receive(&mut a).await;
    receive(&mut b).await;
    let mut data = connect(&url, "role=server&v=2&connectionId=shared").await;
    closed(&mut old, 1008).await;
    data.send(Message::text("fanout")).await.unwrap();
    assert_eq!(receive(&mut a).await, Message::text("fanout"));
    assert_eq!(receive(&mut b).await, Message::text("fanout"));
    a.close(None).await.unwrap();
    b.send(Message::binary(vec![1, 2, 3])).await.unwrap();
    assert_eq!(receive(&mut data).await, Message::binary(vec![1, 2, 3]));
    data.close(None).await.unwrap();
    closed(&mut b, 1012).await;
    assert_eq!(
        json(&mut control).await,
        serde_json::json!({"type":"disconnected","connectionId":"shared"})
    );
    control
        .send(Message::text(r#"{"type":"ping"}"#))
        .await
        .unwrap();
    let pong = json(&mut control).await;
    assert_eq!(pong["type"], "pong");
    assert!(pong["ts"].is_u64());
    control.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn oversized_control_message_is_explicitly_closed_with_1009() {
    let (url, relay, task) = start(Config::default()).await;
    let mut control = connect(&url, "role=server&v=2").await;
    json(&mut control).await;
    control
        .send(Message::text("x".repeat(65537)))
        .await
        .unwrap();
    closed(&mut control, 1009).await;
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn weighted_ingress_exhaustion_closes_only_new_source_and_recovers() {
    let config = Config {
        ingress_budget_bytes: 1024,
        ingress_weight: 4,
        ..Config::default()
    };
    let (url, relay, task) = start(config).await;
    let mut a = connect(&url, "role=client&v=2&connectionId=a").await;
    a.send(Message::binary(vec![1; 256])).await.unwrap();
    timeout(Duration::from_secs(2), async {
        while relay.ingress_reserved_bytes() != 1024 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut b = connect(&url, "role=client&v=2&connectionId=b").await;
    b.send(Message::text("over capacity")).await.unwrap();
    closed(&mut b, 1013).await;
    let mut data = connect(&url, "role=server&v=2&connectionId=a").await;
    assert_eq!(receive(&mut data).await, Message::binary(vec![1; 256]));
    a.close(None).await.unwrap();
    closed(&mut data, 1001).await;
    relay.wait_idle().await;
    assert_eq!(relay.ingress_reserved_bytes(), 0);
    task.abort();
}

#[tokio::test]
async fn slow_fanout_peer_does_not_deadlock_reverse_traffic_or_healthy_order() {
    let config = Config {
        delivery_timeout: Duration::from_millis(300),
        ..Config::default()
    };
    let (url, relay, task) = start(config).await;
    let mut unread = connect(&url, "role=client&v=2&connectionId=fanout").await;
    let mut healthy = connect(&url, "role=client&v=2&connectionId=fanout").await;
    let data = connect(&url, "role=server&v=2&connectionId=fanout").await;
    let (mut sender, mut receiver) = data.split();
    let produce = tokio::spawn(async move {
        for i in 0..16u8 {
            sender
                .send(Message::binary(vec![i; 1024 * 1024]))
                .await
                .unwrap();
        }
        sender
    });
    let mut saw_reverse = false;
    for i in 0..16u8 {
        assert_eq!(
            receive(&mut healthy).await,
            Message::binary(vec![i; 1024 * 1024])
        );
        if i == 4 {
            healthy
                .send(Message::text("reverse under pressure"))
                .await
                .unwrap();
            assert_eq!(
                timeout(Duration::from_secs(2), receiver.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                Message::text("reverse under pressure")
            );
            saw_reverse = true;
        }
    }
    assert!(saw_reverse);
    let mut saw_close = false;
    while let Some(message) = timeout(Duration::from_secs(3), unread.next())
        .await
        .unwrap()
    {
        match message.unwrap() {
            Message::Close(Some(frame)) => {
                assert_eq!(u16::from(frame.code), 1013);
                saw_close = true;
                break;
            }
            Message::Binary(_) => {}
            other => panic!("unexpected {other:?}"),
        }
    }
    assert!(saw_close, "slow peer must receive an explicit close");
    let mut sender = produce.await.unwrap();
    sender.close().await.unwrap();
    closed(&mut healthy, 1012).await;
    relay.wait_idle().await;
    task.abort();
}

#[tokio::test]
async fn exact_maximum_fragmented_payload_delivers_and_one_byte_more_closes_1009() {
    use tokio::io::AsyncWriteExt;
    use tokio_tungstenite::tungstenite::protocol::frame::{
        Frame,
        coding::{Data, OpCode},
    };
    let (url, relay, task) = start(Config::default()).await;
    let mut daemon = connect(&url, "role=server").await;
    let mut client = connect(&url, "role=client").await;
    let maximum = paseo_relay::protocol::MAX_MESSAGE_BYTES;
    let payload = vec![0xa5; maximum];
    let half = maximum / 2;
    client
        .send(Message::Frame(Frame::message(
            payload[..half].to_vec(),
            OpCode::Data(Data::Binary),
            false,
        )))
        .await
        .unwrap();
    client
        .send(Message::Ping(vec![4, 5, 6].into()))
        .await
        .unwrap();
    assert_eq!(
        receive(&mut client).await,
        Message::Pong(vec![4, 5, 6].into())
    );
    client
        .send(Message::Frame(Frame::message(
            payload[half..].to_vec(),
            OpCode::Data(Data::Continue),
            true,
        )))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(35), daemon.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data()
            .as_ref(),
        payload.as_slice()
    );
    // Send only the oversized declared length; the parser must reject before allocating its body.
    let mut header = vec![0x82, 0xff];
    header.extend_from_slice(&((maximum + 1) as u64).to_be_bytes());
    header.extend_from_slice(&[1, 2, 3, 4]);
    client.get_mut().write_all(&header).await.unwrap();
    closed(&mut client, 1009).await;
    daemon.close(None).await.unwrap();
    relay.wait_idle().await;
    task.abort();
}
