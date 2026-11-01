use futures_util::{SinkExt, StreamExt};
use std::{
    io,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::{
    TlsConnector,
    rustls::{self, pki_types::ServerName},
};
use tokio_tungstenite::{client_async, tungstenite::Message};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
async fn start() -> (std::net::SocketAddr, Process) {
    let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_paseo-relay"))
            .env_clear()
            .env("PASEO_RELAY_PORT", address.port().to_string())
            .env("PASEO_RELAY_TLS_CERT_FILE", fixture("localhost.pem"))
            .env("PASEO_RELAY_TLS_KEY_FILE", fixture("localhost.key"))
            .env("PASEO_RELAY_HTTP_IDLE_TIMEOUT_MS", "200")
            .env("PASEO_RELAY_ACCEPTORS", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    timeout(Duration::from_secs(5), async {
        loop {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "exited before listen"
            );
            if TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    (address, process)
}
fn connector(version: &'static rustls::SupportedProtocolVersion, trusted: bool) -> TlsConnector {
    let mut roots = rustls::RootCertStore::empty();
    if trusted {
        for cert in rustls_pemfile::certs(&mut &include_bytes!("fixtures/localhost.pem")[..]) {
            roots.add(cert.unwrap()).unwrap();
        }
    }
    let mut config = rustls::ClientConfig::builder_with_protocol_versions(&[version])
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    TlsConnector::from(Arc::new(config))
}
async fn connect(
    address: std::net::SocketAddr,
    version: &'static rustls::SupportedProtocolVersion,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    timeout(
        Duration::from_secs(3),
        connector(version, true).connect(
            ServerName::try_from("localhost").unwrap(),
            TcpStream::connect(address).await.unwrap(),
        ),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn https_and_wss_forwarding_use_verified_tls12_and_tls13_and_drain() {
    let (address, mut process) = start().await;
    verify_transport(address).await;
    let (mut control, _) = client_async(
        "wss://localhost/ws?serverId=v2&role=server&v=2",
        connect(address, &rustls::version::TLS13).await,
    )
    .await
    .unwrap();
    let sync = control.next().await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(sync.to_text().unwrap()).unwrap()["type"],
        "sync"
    );
    let (mut client, _) = client_async(
        "wss://localhost/ws?serverId=v2&role=client&v=2&connectionId=peer",
        connect(address, &rustls::version::TLS13).await,
    )
    .await
    .unwrap();
    let connected = control.next().await.unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(connected.to_text().unwrap()).unwrap()["type"],
        "connected"
    );
    let (mut data, _) = client_async(
        "wss://localhost/ws?serverId=v2&role=server&v=2&connectionId=peer",
        connect(address, &rustls::version::TLS13).await,
    )
    .await
    .unwrap();
    client.send(Message::text("v2 over TLS")).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), data.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Message::text("v2 over TLS")
    );
    assert!(
        Command::new("kill")
            .args(["-TERM", &process.0.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    timeout(Duration::from_secs(2), async {
        loop {
            let mut tls = connect(address, &rustls::version::TLS13).await;
            tls.write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            let mut output = String::new();
            tls.read_to_string(&mut output).await.unwrap();
            if output.starts_with("HTTP/1.1 503") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    control.close(None).await.unwrap();
    client.close(None).await.unwrap();
    data.close(None).await.unwrap();
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
async fn untrusted_server_plaintext_and_stalled_handshakes_fail_then_recover() {
    let (address, _process) = start().await;
    assert!(
        connector(&rustls::version::TLS13, false)
            .connect(
                ServerName::try_from("localhost").unwrap(),
                TcpStream::connect(address).await.unwrap()
            )
            .await
            .is_err()
    );
    let mut plain = TcpStream::connect(address).await.unwrap();
    plain
        .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut output = Vec::new();
    let _ = timeout(Duration::from_secs(2), plain.read_to_end(&mut output))
        .await
        .unwrap();
    assert!(!output.starts_with(b"HTTP/1.1 200"));
    let mut stalled = TcpStream::connect(address).await.unwrap();
    let mut byte = [0];
    let result = timeout(Duration::from_secs(2), stalled.read(&mut byte))
        .await
        .unwrap();
    assert!(
        matches!(result, Ok(0))
            || matches!(result, Err(ref e) if e.kind() == io::ErrorKind::ConnectionReset)
    );
    let _ = connect(address, &rustls::version::TLS13).await;
}

#[test]
fn invalid_tls_material_exits_instead_of_falling_back_to_plaintext() {
    for (cert, key) in [
        ("localhost.pem", "mismatched.key"),
        ("missing.pem", "localhost.key"),
        ("localhost.pem", "missing.key"),
        ("localhost.key", "localhost.key"),
        ("localhost.pem", "localhost.pem"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_paseo-relay"))
            .env_clear()
            .env("PASEO_RELAY_TLS_CERT_FILE", fixture(cert))
            .env("PASEO_RELAY_TLS_KEY_FILE", fixture(key))
            .output()
            .unwrap();
        assert!(!output.status.success(), "accepted {cert}, {key}");
    }
}

async fn verify_transport(address: std::net::SocketAddr) {
    for version in [&rustls::version::TLS12, &rustls::version::TLS13] {
        let mut tls = connect(address, version).await;
        assert_eq!(tls.get_ref().1.alpn_protocol(), Some(&b"http/1.1"[..]));
        tls.write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut output = String::new();
        timeout(Duration::from_secs(2), tls.read_to_string(&mut output))
            .await
            .unwrap()
            .unwrap();
        assert!(output.starts_with("HTTP/1.1 200"), "{output}");
        let (mut server, _) = client_async(
            "wss://localhost/ws?serverId=tls&role=server",
            connect(address, version).await,
        )
        .await
        .unwrap();
        let (mut client, _) = client_async(
            "wss://localhost/ws?serverId=tls&role=client",
            connect(address, version).await,
        )
        .await
        .unwrap();
        // Upgraded TLS sockets must outlive the pre-upgrade idle timeout and release its only slot.
        tokio::time::sleep(Duration::from_millis(300)).await;
        for reverse in [false, true] {
            let (sender, receiver) = if reverse {
                (&mut client, &mut server)
            } else {
                (&mut server, &mut client)
            };
            let payload = Message::Binary(vec![42; 65536].into());
            sender.send(payload.clone()).await.unwrap();
            assert_eq!(
                timeout(Duration::from_secs(2), receiver.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap(),
                payload
            );
        }
        server.close(None).await.unwrap();
        client.close(None).await.unwrap();
    }
}

struct Container(String);
impl Drop for Container {
    fn drop(&mut self) {
        let _ = Command::new("docker").args(["rm", "-f", &self.0]).output();
    }
}

#[tokio::test]
#[ignore = "requires a built Rust Docker image and a local Docker daemon"]
async fn scratch_image_serves_verified_https_and_wss_with_supplied_pem() {
    let image = std::env::var("PASEO_RELAY_TEST_IMAGE").unwrap_or("paseo-relay-rust:local".into());
    let output = Command::new("docker")
        .args([
            "create",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "-p",
            "127.0.0.1::4000",
            "-e",
            "PASEO_RELAY_TLS_CERT_FILE=/tls/localhost.pem",
            "-e",
            "PASEO_RELAY_TLS_KEY_FILE=/tls/localhost.key",
            &image,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let container = Container(String::from_utf8(output.stdout).unwrap().trim().into());
    assert!(
        Command::new("docker")
            .arg("cp")
            .arg(fixture(""))
            .arg(format!("{}:/tls", container.0))
            .status()
            .unwrap()
            .success()
    );
    assert!(
        Command::new("docker")
            .args(["start", &container.0])
            .status()
            .unwrap()
            .success()
    );
    let output = Command::new("docker")
        .args(["port", &container.0, "4000/tcp"])
        .output()
        .unwrap();
    let address = String::from_utf8(output.stdout)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    verify_transport(address).await;
    assert!(
        Command::new("docker")
            .args(["stop", "--time", "40", &container.0])
            .status()
            .unwrap()
            .success()
    );
    let output = Command::new("docker")
        .args(["inspect", "--format", "{{.State.ExitCode}}", &container.0])
        .output()
        .unwrap();
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), "0");
}
