use std::{
    process::{Child, Command, Stdio},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn invalid_config_exits_without_listening() {
    let output = Command::new(env!("CARGO_BIN_EXE_paseo-relay"))
        .env_clear()
        .env("PASEO_RELAY_PORT", "0")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("PASEO_RELAY_PORT"));
}

#[tokio::test]
async fn linux_binary_serves_probes_and_stops_on_sigterm() {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_paseo-relay"))
            .env_clear()
            .env("PASEO_RELAY_PORT", address.port().to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "binary exited before listen"
            );
            if TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    for (path, body) in [
        ("/health", r#"{"status":"ok"}"#),
        ("/ready", r#"{"status":"ready"}"#),
        ("/metrics", "paseo_relay_active_websockets 0"),
    ] {
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut output = Vec::new();
        timeout(Duration::from_secs(2), socket.read_to_end(&mut output))
            .await
            .unwrap()
            .unwrap();
        let output = String::from_utf8(output).unwrap();
        assert!(output.starts_with("HTTP/1.1 200"));
        assert!(output.contains(body), "{output}");
    }
    assert!(
        Command::new("kill")
            .args(["-TERM", &process.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn linux_rss_watermark_recovers_from_real_fragment_memory_without_configuration_change() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{
            Message,
            protocol::frame::{
                Frame,
                coding::{Data, OpCode},
            },
        },
    };
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_paseo-relay"))
            .env_clear()
            .env("PASEO_RELAY_PORT", address.port().to_string())
            .env("PASEO_RELAY_MEMORY_WATERMARK_BYTES", "268435456")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    timeout(Duration::from_secs(5), async {
        loop {
            assert!(process.0.try_wait().unwrap().is_none());
            if TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let (mut healthy, _) = connect_async(format!("ws://{address}/ws?serverId=healthy&role=server"))
        .await
        .unwrap();
    let mut fragments = Vec::new();
    let mut shed = false;
    for index in 0..40 {
        let connected = connect_async(format!(
            "ws://{address}/ws?serverId=fragment{index}&role=server"
        ))
        .await;
        let Ok((mut socket, _)) = connected else {
            break;
        };
        let _ = socket
            .send(Message::Frame(Frame::message(
                vec![7; 8 * 1024 * 1024],
                OpCode::Data(Data::Binary),
                false,
            )))
            .await;
        fragments.push(socket);
        let metrics = binary_request(address, "/metrics").await;
        let count = metrics
            .lines()
            .find_map(|line| line.strip_prefix("paseo_relay_memory_pressure_disconnects_total "))
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert!(metrics.contains("paseo_relay_ingress_reserved_bytes 0"));
        if count > 0 {
            shed = true;
            break;
        }
    }
    if !shed {
        timeout(Duration::from_secs(3), async {
            loop {
                let metrics = binary_request(address, "/metrics").await;
                if !metrics.contains("paseo_relay_memory_pressure_disconnects_total 0\n") {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("real fragment memory did not trigger pressure");
    }
    drop(fragments);
    timeout(Duration::from_secs(5), async {
        loop {
            if binary_request(address, "/ready")
                .await
                .starts_with("HTTP/1.1 200")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("RSS relief did not restore admission");
    healthy.send(Message::Ping(vec![9].into())).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), healthy.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::Pong(vec![9].into())
    );
    healthy.close(None).await.unwrap();
}

async fn binary_request(address: std::net::SocketAddr, path: &str) -> String {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), socket.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(bytes).unwrap()
}

struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[tokio::test]
#[ignore = "requires a built Rust Docker image and a local Docker daemon"]
async fn static_nonroot_container_serves_websockets_after_restart() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    let image = std::env::var("PASEO_RELAY_TEST_IMAGE").unwrap_or("paseo-relay-rust:local".into());
    let output = Command::new("docker")
        .args(["run", "--detach", "--publish", "127.0.0.1::4000", &image])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let container = Container(String::from_utf8(output.stdout).unwrap().trim().to_owned());
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.Config.User}}", &container.0])
        .output()
        .unwrap();
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        "65532:65532"
    );
    for round in 0..2 {
        if round > 0 {
            assert!(
                Command::new("docker")
                    .args(["restart", &container.0])
                    .stdout(Stdio::null())
                    .status()
                    .unwrap()
                    .success()
            );
        }
        let output = Command::new("docker")
            .args(["port", &container.0, "4000/tcp"])
            .output()
            .unwrap();
        let address: std::net::SocketAddr = String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        timeout(Duration::from_secs(5), async {
            loop {
                if TcpStream::connect(address).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(
            binary_request(address, "/health")
                .await
                .contains(r#"{"status":"ok"}"#)
        );
        assert!(
            binary_request(address, "/ready")
                .await
                .contains(r#"{"status":"ready"}"#)
        );
        let query = format!("ws://{address}/ws?serverId=restart{round}&v=2&connectionId=peer");
        let (mut client, _) = connect_async(format!("{query}&role=client")).await.unwrap();
        let (mut daemon, _) = connect_async(format!("{query}&role=server")).await.unwrap();
        client.send(Message::text("container relay")).await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(3), daemon.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Message::text("container relay")
        );
        daemon.close(None).await.unwrap();
        let _ = daemon.next().await;
        let _ = client.close(None).await;
        timeout(Duration::from_secs(5), async {
            loop {
                let metrics = binary_request(address, "/metrics").await;
                if metrics.contains("paseo_relay_active_websockets 0\n") {
                    assert!(metrics.contains("paseo_relay_ingress_reserved_bytes 0\n"));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
}
