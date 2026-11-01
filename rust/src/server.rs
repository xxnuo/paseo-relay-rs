use super::Relay;
use axum::Extension;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use std::{
    cmp::Reverse,
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::{Instant, Sleep},
};

impl Relay {
    pub async fn serve(
        &self,
        listener: TcpListener,
        shutdown: impl Future<Output = ()>,
    ) -> io::Result<()> {
        let tls = self
            .0
            .config
            .tls
            .as_ref()
            .map(|tls| tls.acceptor())
            .transpose()?;
        let mut connections = JoinSet::new();
        let app = self.router();
        let http_slots = Arc::new(Semaphore::new(self.0.config.http_connection_limit));
        let monitor = self.monitor_memory();
        let cluster = self.run_cluster();
        tokio::pin!(shutdown, monitor, cluster);
        let result = loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break Ok(()),
                result = &mut cluster => { break result; },
                result = &mut monitor => {
                    break result;
                },
                Some(result) = connections.join_next(), if !connections.is_empty() => {
                    if let Err(error) = result {
                        break Err(io::Error::other(error));
                    }
                },
                accepted = async {
                    let permit=http_slots.clone().acquire_owned().await.map_err(io::Error::other)?;
                    let (stream,_)=listener.accept().await?;
                    Ok::<_,io::Error>((stream,permit))
                } => {
                    let (stream, permit) = match accepted { Ok(accepted) => accepted, Err(error) => break Err(error) };
                    if let Err(error) = stream.set_nodelay(true).and_then(|()| socket2::SockRef::from(&stream).set_recv_buffer_size(self.0.config.tcp_receive_buffer_bytes)) { break Err(error); }
                    let upgraded = Arc::new(HttpLease { upgraded:AtomicBool::new(false),permit:Mutex::new(Some(permit)) });
                    let app = app.clone().layer(Extension(upgraded.clone()));
                    let tls = tls.clone();
                    let idle_timeout = self.0.config.http_idle_timeout;
                    connections.spawn(async move {
                        if let Some(tls) = tls {
                            // Keep the HTTP permit throughout a bounded TLS handshake.
                            if let Ok(Ok(stream)) = tokio::time::timeout(idle_timeout, tls.accept(stream)).await {
                                serve_http(stream, app, upgraded, idle_timeout).await;
                            }
                        } else {
                            serve_http(stream, app, upgraded, idle_timeout).await;
                        }
                    });
                },
            }
        };
        if result.is_err() {
            self.begin_drain();
            self.close_all();
            let _ = tokio::time::timeout(Duration::from_secs(2), async {
                while self.active_connections() != 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await;
        }
        result
    }

    pub fn set_memory_watermark(&self, bytes: usize) {
        let mut state = self.0.pressure.lock().unwrap();
        state.watermark = bytes;
        if bytes == 0 {
            state.active = false;
        }
    }

    async fn monitor_memory(&self) -> io::Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            let mut pressure = self.0.pressure.lock().unwrap();
            if pressure.watermark == 0 {
                pressure.active = false;
                continue;
            }
            let memory = resident_bytes()?;
            let recovery = pressure
                .watermark
                .saturating_sub(crate::protocol::MAX_MESSAGE_BYTES);
            if pressure.active && memory <= recovery {
                pressure.active = false;
            }
            if memory < pressure.watermark && !pressure.active {
                continue;
            }
            pressure.active = true;
            let mut peers: Vec<_> = self
                .0
                .sessions
                .lock()
                .unwrap()
                .values()
                .flat_map(|session| session.peers())
                .filter(|peer| peer.close.borrow().is_none())
                .collect();
            peers.sort_unstable_by_key(|peer| {
                (
                    Reverse(peer.delivering.load(Ordering::Relaxed)),
                    Reverse(peer.id),
                )
            });
            // RSS is remeasured between bounded batches; parser buffers are not byte-ledger reservations.
            let count = memory
                .saturating_sub(recovery)
                .div_ceil(crate::protocol::MAX_MESSAGE_BYTES)
                .clamp(1, 32);
            for peer in peers.into_iter().take(count) {
                peer.close(1013, "Relay memory pressure");
            }
        }
    }
}

pub(super) fn resident_bytes() -> io::Result<usize> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|line| line.split_whitespace().next())
        .and_then(|value| value.parse::<usize>().ok())
        .and_then(|kib| kib.checked_mul(1024))
        .ok_or_else(|| io::Error::other("missing process RSS"))
}

async fn serve_http<T>(stream: T, app: axum::Router, upgraded: Arc<HttpLease>, timeout: Duration)
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let stream = IdleIo {
        stream,
        upgraded,
        timeout,
        timer: Box::pin(tokio::time::sleep(timeout)),
    };
    let _ = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(stream), TowerToHyperService::new(app))
        .with_upgrades()
        .await;
}

struct IdleIo<T> {
    stream: T,
    upgraded: Arc<HttpLease>,
    timeout: Duration,
    timer: Pin<Box<Sleep>>,
}

impl<T: AsyncRead + Unpin> AsyncRead for IdleIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buf);
        if self.upgraded.upgraded.load(Ordering::Acquire) {
            return result;
        }
        if buf.filled().len() > before {
            let deadline = Instant::now() + self.timeout;
            self.timer.as_mut().reset(deadline);
        } else if result.is_pending() && self.timer.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP idle timeout",
            )));
        }
        result
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for IdleIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

pub(super) struct HttpLease {
    upgraded: AtomicBool,
    permit: Mutex<Option<OwnedSemaphorePermit>>,
}
impl HttpLease {
    pub(super) fn upgrade(&self) {
        self.upgraded.store(true, Ordering::Release);
        self.permit.lock().unwrap().take();
    }
}
