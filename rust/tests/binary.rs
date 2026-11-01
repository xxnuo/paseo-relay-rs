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
    try_binary_request(address, path).await.unwrap()
}

async fn try_binary_request(address: std::net::SocketAddr, path: &str) -> std::io::Result<String> {
    let mut socket = TcpStream::connect(address).await?;
    socket
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .await?;
    let mut bytes = Vec::new();
    timeout(Duration::from_secs(3), socket.read_to_end(&mut bytes))
        .await
        .map_err(std::io::Error::other)??;
    String::from_utf8(bytes).map_err(std::io::Error::other)
}

async fn wait_container_ready(address: std::net::SocketAddr) {
    timeout(Duration::from_secs(8), async {
        loop {
            // Docker's port proxy can accept TCP before Nginx has bound its listener.
            if let Ok(response) = try_binary_request(address, "/ready").await
                && response.starts_with("HTTP/1.1 200")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("container did not become HTTP-ready");
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
async fn nginx_nonroot_container_serves_websockets_after_restart() {
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
        wait_container_ready(address).await;
        let workers = Command::new("docker")
            .args(["top", &container.0, "-eo", "pid,args"])
            .output()
            .unwrap();
        assert!(workers.status.success());
        assert_eq!(
            String::from_utf8(workers.stdout)
                .unwrap()
                .matches("nginx: worker process")
                .count(),
            1
        );
        let health = binary_request(address, "/health").await;
        assert!(health.contains(r#"{"status":"ok"}"#));
        assert!(
            health.to_ascii_lowercase().contains("server: nginx"),
            "Nginx was bypassed: {health}"
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
        if round == 0 {
            // The proxy must not impose Nginx's default 60-second idle cutoff.
            tokio::time::sleep(Duration::from_secs(61)).await;
            client
                .send(Message::text("after proxy idle"))
                .await
                .unwrap();
            assert_eq!(
                timeout(Duration::from_secs(3), daemon.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                Message::text("after proxy idle")
            );
        }
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

#[tokio::test]
#[ignore = "requires a built Rust + Nginx image and a local Docker daemon"]
async fn nginx_container_preserves_drain_and_exits_if_either_service_dies() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{connect_async, tungstenite::Message};
    let image = std::env::var("PASEO_RELAY_TEST_IMAGE").unwrap_or("paseo-relay-rust:local".into());
    for scenario in ["drain", "nginx", "relay"] {
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--publish",
                "127.0.0.1::4001",
                "--env",
                "PASEO_RELAY_PORT=4001",
                "--env",
                "PASEO_RELAY_NGINX_WORKERS=3",
                "--stop-timeout",
                "40",
                &image,
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        let container = Container(String::from_utf8(output.stdout).unwrap().trim().to_owned());
        let output = Command::new("docker")
            .args(["port", &container.0, "4001/tcp"])
            .output()
            .unwrap();
        let address = String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        wait_container_ready(address).await;
        let workers = Command::new("docker")
            .args(["top", &container.0, "-eo", "pid,args"])
            .output()
            .unwrap();
        assert!(workers.status.success());
        assert_eq!(
            String::from_utf8(workers.stdout)
                .unwrap()
                .matches("nginx: worker process")
                .count(),
            3
        );
        let output = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                &container.0,
            ])
            .output()
            .unwrap();
        let ip = String::from_utf8(output.stdout).unwrap();
        assert!(
            !Command::new("docker")
                .args([
                    "exec",
                    &container.0,
                    "wget",
                    "-q",
                    "-T",
                    "1",
                    "-O",
                    "/dev/null",
                    &format!("http://{}:4002/health", ip.trim())
                ])
                .status()
                .unwrap()
                .success(),
            "backend must be loopback-only"
        );
        if scenario == "drain" {
            let (mut daemon, _) =
                connect_async(format!("ws://{address}/ws?serverId=drain&role=server"))
                    .await
                    .unwrap();
            let (mut client, _) =
                connect_async(format!("ws://{address}/ws?serverId=drain&role=client"))
                    .await
                    .unwrap();
            assert!(
                Command::new("docker")
                    .args(["kill", "--signal=TERM", &container.0])
                    .output()
                    .unwrap()
                    .status
                    .success()
            );
            timeout(Duration::from_secs(3), async {
                while !binary_request(address, "/ready")
                    .await
                    .starts_with("HTTP/1.1 503")
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
            client.send(Message::text("during drain")).await.unwrap();
            assert_eq!(
                timeout(Duration::from_secs(3), daemon.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                Message::text("during drain")
            );
            daemon.close(None).await.unwrap();
            client.close(None).await.unwrap();
        } else {
            let kill = if scenario == "nginx" {
                "kill -TERM \"$(cat /tmp/nginx.pid)\""
            } else {
                "kill -KILL \"$(pidof paseo-relay)\""
            };
            assert!(
                Command::new("docker")
                    .args(["exec", &container.0, "sh", "-c", kill])
                    .output()
                    .unwrap()
                    .status
                    .success()
            );
        }
        let state = timeout(Duration::from_secs(8), async {
            loop {
                let output = Command::new("docker")
                    .args([
                        "inspect",
                        "--format",
                        "{{.State.Running}} {{.State.ExitCode}}",
                        &container.0,
                    ])
                    .output()
                    .unwrap();
                let state = String::from_utf8(output.stdout).unwrap();
                if state.starts_with("false ") {
                    break state;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("one failed service left the container running");
        assert_eq!(
            state.trim(),
            if scenario == "drain" {
                "false 0"
            } else {
                "false 1"
            }
        );
    }
}

struct Network(String);
impl Drop for Network {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["network", "rm", &self.0])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

#[tokio::test]
#[ignore = "requires a built Rust Docker image and a local Docker daemon"]
async fn static_cluster_uses_real_container_dns_and_reroutes_public_websockets() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{Error, Message},
    };
    let image = std::env::var("PASEO_RELAY_TEST_IMAGE").unwrap_or("paseo-relay-rust:local".into());
    let network = Network(format!("paseo-rust-cluster-test-{}", std::process::id()));
    assert!(
        Command::new("docker")
            .args(["network", "create", &network.0])
            .stdout(Stdio::null())
            .status()
            .unwrap()
            .success()
    );
    let mut containers = Vec::new();
    let mut addresses = Vec::new();
    for (name, peer) in [("a", "b"), ("b", "a")] {
        let output = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--network",
                &network.0,
                "--network-alias",
                name,
                "--publish",
                "127.0.0.1::4000",
                "--env",
                "PASEO_RELAY_CLUSTER_LISTEN=0.0.0.0:4400",
                "--env",
                "PASEO_RELAY_MIN_CLUSTER_SIZE=2",
                "--env",
                "TOKIO_WORKER_THREADS=2",
                "--env",
                "RELEASE_COOKIE=test-only-docker-cookie-at-least-32-bytes",
                "--env",
                &format!("RELEASE_NODE={name}@{name}:4400"),
                "--env",
                &format!("PASEO_RELAY_OWNERSHIP_TARGET=target-{name}"),
                "--env",
                &format!("PASEO_RELAY_CLUSTER_NODES={peer}@{peer}:4400"),
                &image,
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let container = Container(String::from_utf8(output.stdout).unwrap().trim().to_owned());
        let output = Command::new("docker")
            .args(["port", &container.0, "4000/tcp"])
            .output()
            .unwrap();
        addresses.push(
            String::from_utf8(output.stdout)
                .unwrap()
                .trim()
                .parse::<std::net::SocketAddr>()
                .unwrap(),
        );
        containers.push(container);
    }
    for address in &addresses {
        wait_container_ready(*address).await;
    }
    let (mut daemon, _) = connect_async(format!(
        "ws://{}/ws?serverId=docker-cluster&role=server",
        addresses[1]
    ))
    .await
    .unwrap();
    daemon.send(Message::Ping(vec![1].into())).await.unwrap();
    daemon.next().await.unwrap().unwrap();
    let owner = timeout(Duration::from_secs(8), async {
        loop {
            for (landing, address) in addresses.iter().enumerate() {
                match connect_async(format!(
                    "ws://{}/ws?serverId=docker-cluster&role=client",
                    address
                ))
                .await
                {
                    Err(Error::Http(response)) if response.status() == 409 => {
                        let owner = 1 - landing;
                        assert_eq!(
                            response.headers()["x-reroute-target"],
                            format!("target-{}", if owner == 0 { "a" } else { "b" })
                        );
                        return owner;
                    }
                    Ok((mut temporary, _)) => {
                        temporary.close(None).await.unwrap();
                    }
                    other => panic!("unexpected {other:?}"),
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    if owner == 0 {
        let Message::Close(Some(close)) = timeout(Duration::from_secs(3), daemon.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        else {
            panic!("losing daemon was not fenced");
        };
        assert_eq!(u16::from(close.code), 1012);
        daemon = connect_async(format!(
            "ws://{}/ws?serverId=docker-cluster&role=server",
            addresses[owner]
        ))
        .await
        .unwrap()
        .0;
        daemon.send(Message::Ping(vec![2].into())).await.unwrap();
        daemon.next().await.unwrap().unwrap();
    }
    let (mut client, _) = connect_async(format!(
        "ws://{}/ws?serverId=docker-cluster&role=client",
        addresses[owner]
    ))
    .await
    .unwrap();
    client
        .send(Message::text("replayed to owner"))
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), daemon.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::text("replayed to owner")
    );
    daemon.send(Message::binary(vec![4, 5, 6])).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), client.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::binary(vec![4, 5, 6])
    );
    daemon.close(None).await.unwrap();
    client.close(None).await.unwrap();
    drop(containers);
}

#[tokio::test]
async fn large_json_metadata_does_not_expand_into_an_unbounded_value_tree() {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        connect_async_with_config,
        tungstenite::{Message, protocol::WebSocketConfig},
    };
    let (mut process, address) = limited_relay().await;
    let ws_config =
        WebSocketConfig::default().max_frame_size(Some(paseo_relay::protocol::MAX_MESSAGE_BYTES));
    let (mut daemon, _) = connect_async_with_config(
        format!("ws://{address}/ws?serverId=json&role=server"),
        Some(ws_config),
        true,
    )
    .await
    .unwrap();
    let (mut client, _) = connect_async_with_config(
        format!("ws://{address}/ws?serverId=json&role=client"),
        Some(ws_config),
        true,
    )
    .await
    .unwrap();
    for kind in ["hello", "opaque"] {
        let payload = format!(
            "{{\"type\":\"{kind}\",\"key\":\"CQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\",\"padding\":[{}0]}}",
            "0,".repeat(12 * 1024 * 1024)
        );
        client.send(Message::text(payload.clone())).await.unwrap();
        let message = timeout(Duration::from_secs(15), daemon.next())
            .await
            .unwrap()
            .unwrap()
            .expect("JSON input killed the relay under its fixed address-space budget");
        assert!(
            message.into_data().as_ref() == payload.as_bytes(),
            "payload changed"
        );
    }
    assert!(process.0.try_wait().unwrap().is_none());
    assert!(
        binary_request(address, "/ready")
            .await
            .starts_with("HTTP/1.1 200")
    );
    daemon.close(None).await.unwrap();
    client.close(None).await.unwrap();
}

#[tokio::test]
async fn cancelled_queued_sources_cannot_retain_uncharged_payloads() {
    use futures_util::SinkExt;
    use tokio_tungstenite::{MaybeTlsStream, connect_async, tungstenite::Message};
    let (mut process, address) = limited_relay().await;
    let route = format!("ws://{address}/ws?serverId=queued-memory&v=2&connectionId=peer");
    let (mut daemon, _) = connect_async(format!("{route}&role=server")).await.unwrap();
    let (mut active, _) = connect_async(format!("{route}&role=client")).await.unwrap();
    active
        .send(Message::binary(vec![7; 8 * 1024 * 1024]))
        .await
        .unwrap();
    let MaybeTlsStream::Plain(tcp) = daemon.get_ref() else {
        panic!("expected TCP");
    };
    let mut header = [0; 1];
    timeout(Duration::from_secs(3), tcp.peek(&mut header))
        .await
        .unwrap()
        .unwrap();
    for _ in 0..100 {
        let (mut queued, _) = connect_async(format!("{route}&role=client")).await.unwrap();
        queued
            .send(Message::binary(vec![8; 8 * 1024 * 1024]))
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                let text = binary_request(address, "/metrics").await;
                if text.contains("paseo_relay_ingress_reserved_bytes 67108864\n") {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let MaybeTlsStream::Plain(tcp) = queued.get_ref() else {
            panic!("expected TCP");
        };
        socket2::SockRef::from(tcp)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
        drop(queued);
        // Reverse traffic makes the dead source's independent Writer observe its real TCP reset.
        daemon.send(Message::text("reverse")).await.unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                let text = binary_request(address, "/metrics").await;
                if text.contains("paseo_relay_active_websockets 2\n")
                    && text.contains("paseo_relay_ingress_reserved_bytes 33554432\n")
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    assert!(
        process.0.try_wait().unwrap().is_none(),
        "cancelled queue entries retained payload bytes outside the ledger"
    );
}

async fn limited_relay() -> (Process, std::net::SocketAddr) {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut process = Process(
        Command::new("bash")
            .args([
                "-c",
                "ulimit -v 524288 && exec \"$1\"",
                "paseo-json-memory-test",
                env!("CARGO_BIN_EXE_paseo-relay"),
            ])
            .env_clear()
            .env("TOKIO_WORKER_THREADS", "2")
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
                "limited child exited before listening"
            );
            if TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    (process, address)
}
