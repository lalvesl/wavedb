//! Latency collection and the derived distribution.
//!
//! Every sample is kept rather than bucketed: at benchmark sizes the memory is
//! trivial and p99/max come out exact. A mean would hide precisely the settle
//! and checkpoint pauses worth knowing about (RFC 0060 §4).

use std::time::Instant;

/// One phase's timing samples, in nanoseconds.
pub struct Latencies {
    samples: Vec<u64>,
}

impl Latencies {
    #[must_use]
    pub fn with_capacity(n: usize) -> Self {
        Self {
            samples: Vec::with_capacity(n),
        }
    }

    /// Time one operation and record it.
    pub fn time<T>(&mut self, op: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let out = op();
        self.samples.push(start.elapsed().as_nanos() as u64);
        out
    }

    /// The distribution, sorted in place. Consumes the collector because the
    /// samples are reordered — nothing may append afterwards.
    #[must_use]
    pub fn finish(mut self) -> Distribution {
        self.samples.sort_unstable();
        let n = self.samples.len();
        let total: u64 = self.samples.iter().sum();
        Distribution {
            count: n as u64,
            total_ns: total,
            p50_ns: self.at(0.50),
            p95_ns: self.at(0.95),
            p99_ns: self.at(0.99),
            max_ns: self.samples.last().copied().unwrap_or(0),
        }
    }

    fn at(&self, q: f64) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
        let idx = ((self.samples.len() - 1) as f64 * q) as usize;
        self.samples[idx]
    }
}

/// What a phase's timings came to.
#[derive(Debug, Clone, Copy)]
pub struct Distribution {
    pub count: u64,
    pub total_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

impl Distribution {
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn ops_per_sec(&self) -> f64 {
        if self.total_ns == 0 {
            return 0.0;
        }
        self.count as f64 * 1e9 / self.total_ns as f64
    }
}

/// One measured phase of one system's run.
pub struct Phase {
    pub name: &'static str,
    pub dist: Distribution,
    /// Bytes this process wrote to disk during the phase (`/proc/self/io`),
    /// which for the embedded bracket is exactly the engine's own writes.
    pub bytes_written: u64,
}

/// `write_bytes` from `/proc/self/io` — bytes actually sent to the block layer,
/// not `wchar` (which counts writes absorbed by the page cache).
#[must_use]
pub fn disk_write_bytes() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/self/io") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("write_bytes:") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

/// Whose disk writes a phase should be attributed to.
///
/// The embedded bracket writes in this process; the server bracket writes in
/// the server's. Reading `self` for a server row would report a flat zero for
/// every phase, which is a wrong number rather than a missing one.
#[derive(Debug, Clone, Copy)]
pub enum Writer {
    Current,
    Pid(u32),
}

impl Writer {
    /// This writer's cumulative disk-write counter, right now.
    #[must_use]
    pub fn bytes(self) -> u64 {
        match self {
            Self::Current => disk_write_bytes(),
            Self::Pid(pid) => crate::systems::server::write_bytes(pid),
        }
    }
}

/// Run `op` as one phase, bracketing it with this process's disk-write counter.
pub fn phase(
    name: &'static str,
    op: impl FnOnce(&mut Latencies),
    capacity: usize,
) -> Phase {
    phase_of(name, Writer::Current, op, capacity)
}

/// Run `op` as one phase, attributing disk writes to `writer`.
pub fn phase_of(
    name: &'static str,
    writer: Writer,
    op: impl FnOnce(&mut Latencies),
    capacity: usize,
) -> Phase {
    let mut lat = Latencies::with_capacity(capacity);
    let before = writer.bytes();
    op(&mut lat);
    let after = writer.bytes();
    Phase {
        name,
        dist: lat.finish(),
        bytes_written: after.saturating_sub(before),
    }
}
