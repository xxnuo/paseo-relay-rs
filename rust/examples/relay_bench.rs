use futures_util::{SinkExt, StreamExt};
use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, sync::Barrier, time::timeout};
use tokio_tungstenite::{WebSocketStream, client_async, tungstenite::Message};

type Socket = WebSocketStream<TcpStream>;

async fn connect(address: &str, path: &str) -> Socket {
    let tcp = TcpStream::connect(address).await.expect("connect relay");
    tcp.set_nodelay(true).unwrap();
    client_async(format!("ws://{address}/ws?{path}"), tcp)
        .await
        .expect("WebSocket upgrade")
        .0
}

async fn exchange(source: &mut Socket, destination: &mut Socket, payload: &[u8]) {
    source
        .send(Message::binary(payload.to_vec()))
        .await
        .expect("send");
    let message = timeout(Duration::from_secs(10), destination.next())
        .await
        .expect("delivery deadline")
        .expect("destination closed")
        .expect("read frame");
    assert_eq!(
        message,
        Message::binary(payload.to_vec()),
        "loss, corruption or ordering failure"
    );
}

fn percentile(sorted: &[u64], percent: usize) -> u64 {
    sorted[(sorted.len() * percent)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len() - 1)]
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 6 {
        return Err("usage: relay_bench HOST:PORT PAIRS SECONDS PAYLOAD_BYTES v1|v2".into());
    }
    let address = args[1].clone();
    let pairs: usize = args[2].parse()?;
    let seconds: u64 = args[3].parse()?;
    let bytes: usize = args[4].parse()?;
    let version: u8 = match args[5].as_str() {
        "v1" => 1,
        "v2" => 2,
        _ => return Err("invalid protocol".into()),
    };
    assert!((1..=20000).contains(&pairs) && (1..=3600).contains(&seconds));
    assert!((16..=paseo_relay::protocol::MAX_MESSAGE_BYTES).contains(&bytes));
    let mut random = [0; 8];
    getrandom::fill(&mut random)?;
    let run_id = u64::from_le_bytes(random);
    let barrier = Arc::new(Barrier::new(pairs + 1));
    let started = Instant::now();
    let mut tasks = Vec::new();
    for pair in 0..pairs {
        let barrier = barrier.clone();
        let address = address.clone();
        tasks.push(tokio::spawn(async move {
            let query = format!("serverId=bench_{run_id:x}_{pair}&v={version}&connectionId=pair");
            let mut daemon = connect(&address, &format!("{query}&role=server")).await;
            let mut client = connect(&address, &format!("{query}&role=client")).await;
            let mut payload = vec![0xa5; bytes];
            for _ in 0..20 {
                exchange(&mut client, &mut daemon, &payload).await;
                exchange(&mut daemon, &mut client, &payload).await;
            }
            barrier.wait().await;
            let start = Instant::now();
            let mut latencies = Vec::new();
            let mut sequence = 0u64;
            while start.elapsed() < Duration::from_secs(seconds) {
                payload[..8].copy_from_slice(&sequence.to_le_bytes());
                payload[8] = 0;
                let frame_start = Instant::now();
                exchange(&mut client, &mut daemon, &payload).await;
                latencies.push(frame_start.elapsed().as_micros() as u64);
                payload[8] = 1;
                let frame_start = Instant::now();
                exchange(&mut daemon, &mut client, &payload).await;
                latencies.push(frame_start.elapsed().as_micros() as u64);
                sequence += 1;
            }
            let elapsed = start.elapsed();
            // Drain the data socket first so v2 detach cannot race client closure.
            daemon.close(None).await.unwrap();
            let _ = timeout(Duration::from_secs(2), daemon.next()).await;
            let _ = client.close(None).await;
            (elapsed, latencies)
        }));
    }
    timeout(Duration::from_secs(60), barrier.wait()).await?;
    let setup = started.elapsed();
    let mut latencies = Vec::new();
    let mut elapsed = Duration::ZERO;
    for task in tasks {
        let (duration, mut samples) = task.await?;
        elapsed = elapsed.max(duration);
        latencies.append(&mut samples);
    }
    latencies.sort_unstable();
    assert!(!latencies.is_empty());
    println!(
        "{}",
        serde_json::json!({
            "protocol":version,"pairs":pairs,"payload_bytes":bytes,"requested_seconds":seconds,
            "measured_seconds":elapsed.as_secs_f64(),"setup_seconds":setup.as_secs_f64(),
            "forwarded_frames":latencies.len(),"frames_per_second":latencies.len() as f64/elapsed.as_secs_f64(),
            "payload_bytes_per_second":latencies.len() as f64 * bytes as f64/elapsed.as_secs_f64(),
            "latency_us":{"p50":percentile(&latencies,50),"p95":percentile(&latencies,95),"p99":percentile(&latencies,99),"max":latencies.last()},
            "loss":0,"corruption":0,"ordering_errors":0
        })
    );
    Ok(())
}

#[test]
fn percentile_is_nearest_rank_with_defined_small_sample_behavior() {
    assert_eq!(percentile(&[7], 99), 7);
    assert_eq!(percentile(&[1, 2, 3, 4], 50), 2);
    assert_eq!(percentile(&[1, 2, 3, 4], 99), 4);
}
