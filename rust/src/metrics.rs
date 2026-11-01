use crate::protocol::{Handshake, MAX_MESSAGE_BYTES};
use std::{
    fmt::Write,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[derive(Default)]
pub(crate) struct Metrics {
    pub frames: AtomicU64,
    pub bytes: AtomicU64,
    pub rejected: AtomicU64,
    pub reroutes: AtomicU64,
    pub slow: AtomicU64,
    pub timeouts: AtomicU64,
    pub pressure_closes: AtomicU64,
    pub inflight: AtomicU64,
    pub backpressured: AtomicU64,
    max_frame: AtomicU64,
    frame_size: Histogram,
    delivery_wait: Histogram,
    handshakes: [[[AtomicU64; 2]; 2]; 2],
}

#[derive(Default)]
struct Histogram {
    buckets: [AtomicU64; 5],
    count: AtomicU64,
    sum: AtomicU64,
}

impl Histogram {
    fn observe(&self, value: u64, limits: [u64; 5]) {
        self.sum.fetch_add(value, Ordering::Relaxed);
        for (bucket, limit) in self.buckets.iter().zip(limits) {
            if value <= limit {
                bucket.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn render(&self, out: &mut String, name: &str, help: &str, labels: [&str; 5], divisor: f64) {
        writeln!(
            out,
            "# HELP paseo_relay_{name} {help}\n# TYPE paseo_relay_{name} histogram"
        )
        .unwrap();
        for (bucket, label) in self.buckets.iter().zip(labels) {
            writeln!(
                out,
                "paseo_relay_{name}_bucket{{le=\"{label}\"}} {}",
                bucket.load(Ordering::Relaxed)
            )
            .unwrap();
        }
        let count = self.count.load(Ordering::Relaxed);
        writeln!(out,"paseo_relay_{name}_bucket{{le=\"+Inf\"}} {count}\npaseo_relay_{name}_sum {}\npaseo_relay_{name}_count {count}",self.sum.load(Ordering::Relaxed) as f64/divisor).unwrap();
    }
}

pub(crate) fn scalar(out: &mut String, name: &str, value: u64) {
    let kind = if name.ends_with("_total") {
        "counter"
    } else {
        "gauge"
    };
    writeln!(out,"# HELP paseo_relay_{name} {}.\n# TYPE paseo_relay_{name} {kind}\npaseo_relay_{name} {value}",name.replace('_'," ")).unwrap();
}

impl Metrics {
    pub fn frame(&self, bytes: usize) {
        self.max_frame.fetch_max(bytes as u64, Ordering::Relaxed);
        self.frame_size.observe(
            bytes as u64,
            [1024, 65536, 1048576, 8388608, MAX_MESSAGE_BYTES as u64],
        );
    }

    pub fn handshake(&self, version: u8, value: Handshake) {
        let outcome = usize::from(!value.accepted);
        let kind = usize::from(value.kind == "e2ee_hello");
        self.handshakes[outcome][version as usize - 1][kind].fetch_add(1, Ordering::Relaxed);
    }

    pub fn delivery(&self, wait: Duration) {
        self.delivery_wait.observe(
            wait.as_micros() as u64,
            [1000, 10000, 100000, 1000000, 10000000],
        );
    }

    pub fn render(&self, out: &mut String) {
        for (name, counter) in [
            ("frames_forwarded_total", &self.frames),
            ("bytes_forwarded_total", &self.bytes),
            ("connection_rejections_total", &self.rejected),
            ("reroute_responses_total", &self.reroutes),
            ("slow_consumer_disconnects_total", &self.slow),
            ("delivery_timeouts_total", &self.timeouts),
            ("memory_pressure_disconnects_total", &self.pressure_closes),
            ("inflight_delivery_bytes", &self.inflight),
            ("backpressured_sources", &self.backpressured),
            ("max_frame_bytes", &self.max_frame),
        ] {
            scalar(out, name, counter.load(Ordering::Relaxed));
        }
        for (outcome, label) in ["accepted", "rejected"].iter().enumerate() {
            writeln!(out,"# HELP paseo_relay_handshake_{label}_total Client handshake frames {label}.\n# TYPE paseo_relay_handshake_{label}_total counter").unwrap();
            for version in 0..2 {
                for (kind, name) in ["hello", "e2ee_hello"].iter().enumerate() {
                    writeln!(out,"paseo_relay_handshake_{label}_total{{routing_version=\"v{}\",type=\"{name}\"}} {}",version+1,self.handshakes[outcome][version][kind].load(Ordering::Relaxed)).unwrap();
                }
            }
        }
        let maximum = MAX_MESSAGE_BYTES.to_string();
        self.frame_size.render(
            out,
            "frame_size_bytes",
            "WebSocket payload-size distribution.",
            ["1024", "65536", "1048576", "8388608", &maximum],
            1.0,
        );
        self.delivery_wait.render(
            out,
            "delivery_wait_seconds",
            "Time a source waits for synchronous downstream delivery.",
            ["0.001", "0.01", "0.1", "1", "10"],
            1_000_000.0,
        );
    }
}
