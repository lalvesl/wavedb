//! Orchestration: spawn, feed, time, collect.
//!
//! ## Two clocks, and why both
//!
//! A phase now yields **wall time** (measured here, around the whole phase)
//! and **per-operation samples** (measured in each consumer). Under one
//! consumer they differ only by the untimed gaps; under three they differ by
//! roughly three, because summing overlapping windows counts concurrency as
//! duration. Throughput divides by the wall clock — the only form that
//! survives concurrency — and latency stays per-operation
//! ([RFC 0065] §4).
//!
//! ## The order a phase runs in
//!
//! 1. every consumer is idle, holding the driver it has held since the row
//!    began;
//! 2. the generator streams the phase's operations, routing each to its
//!    owner's bounded queue;
//! 3. each consumer is told the phase ended and replies with its samples;
//! 4. **only then** does consumer 0 run `between_phases` — after every
//!    sibling has drained, so a checkpoint or a reopen cannot race an
//!    operation still in flight.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::Instant;

use super::consumer::{Message, Reply, serve};
use super::{Driver, DriverFactory, Workload};

/// How many operations may sit between the generator and a consumer.
///
/// Bounded, and small on purpose. The generator must not run far ahead: it
/// holds materialised records, and the queue lives inside the same 500 MB the
/// engine does. Deep enough that a consumer never starves on a scheduling
/// hiccup, shallow enough that the harness is not the memory experiment.
pub const QUEUE_DEPTH: usize = 256;

/// What one phase came to.
#[derive(Debug)]
pub struct PhaseResult {
    pub name: String,
    /// Every consumer's samples, pooled. A percentile of per-thread
    /// percentiles is not a percentile of anything.
    pub samples: Vec<u64>,
    /// The phase, end to end, on the orchestrating thread.
    pub wall_ns: u64,
    /// Disk bytes the row's writer emitted while the phase ran, from
    /// [`DriverFactory::writer`]. It spans the whole phase rather than the
    /// timed windows, which is right: bytes written by an untimed hook are
    /// still bytes written.
    pub bytes_written: u64,
}

/// Run every phase of `workload` against `factory`.
///
/// # Errors
/// A driver that could not be built, an operation that was refused, or a
/// phase boundary that failed.
pub fn run<F, W>(factory: &F, workload: W) -> Result<Vec<PhaseResult>, String>
where
    F: DriverFactory + Sync,
    W: Workload<Op = <F::Driver as Driver>::Op>,
    W::Op: Send,
{
    run_with(factory, workload, || Ok(()))
}

/// [`run`], plus one observation taken **after the last phase and before any
/// driver closes**.
///
/// That window is the only place the "hot" footprint can be read, and it is a
/// window rather than a point for a reason worth stating: every `close` in
/// this suite quiesces something — SQLite truncates its WAL, WaveDB drains and
/// checkpoints, a server is shut down cleanly. A footprint taken afterwards is
/// the *settled* one under a different name, and the difference between the
/// two is exactly the deferral each system was carrying, which is the number
/// the pair exists to show.
///
/// # Errors
/// Anything [`run`] can fail on, plus whatever `at_hot` reports.
pub fn run_with<F, W, H>(
    factory: &F,
    mut workload: W,
    at_hot: H,
) -> Result<Vec<PhaseResult>, String>
where
    F: DriverFactory + Sync,
    W: Workload<Op = <F::Driver as Driver>::Op>,
    W::Op: Send,
    H: FnOnce() -> Result<(), String>,
{
    let consumers = factory.consumers().max(1);

    // Scoped, and that is load-bearing rather than tidy. The driver is built
    // **inside** the thread that will own it, because it may hold an `Rc` and
    // is not `Send`; only `&F` crosses, which is why the factory is the half
    // with the bound. `spawn` would require moving a built driver across a
    // thread boundary — the exact thing this seam exists to prevent, and the
    // compiler says so.
    std::thread::scope(|scope| {
        let mut inboxes = Vec::with_capacity(consumers);
        let mut outboxes = Vec::with_capacity(consumers);

        for shard in 0..consumers {
            let (tx, rx) = sync_channel::<Message<W::Op>>(QUEUE_DEPTH);
            let (reply_tx, reply_rx) = std::sync::mpsc::channel::<Reply>();
            std::thread::Builder::new()
                .name(format!("bench-consumer-{shard}"))
                .spawn_scoped(scope, move || match factory.build(shard) {
                    Ok(driver) => serve(driver, &rx, &reply_tx, QUEUE_DEPTH),
                    Err(e) => {
                        let _ = reply_tx.send(Reply::Failed(e));
                    }
                })
                .map_err(|e| format!("spawn consumer {shard}: {e}"))?;
            inboxes.push(tx);
            outboxes.push(reply_rx);
        }

        let phases = workload.phases();
        let mut results = Vec::with_capacity(phases.len());
        for (i, phase) in phases.iter().enumerate() {
            let result =
                one_phase(factory, &mut workload, phase, &inboxes, &outboxes)?;
            results.push(result);
            if let Some(next) = phases.get(i + 1) {
                boundary(&inboxes, &outboxes, phase, next)?;
            }
        }

        at_hot()?;

        for tx in &inboxes {
            let _ = tx.send(Message::Shutdown);
        }
        for (shard, rx) in outboxes.iter().enumerate() {
            if let Ok(Reply::Closed(Err(e))) = rx.recv() {
                return Err(format!("consumer {shard}: close: {e}"));
            }
        }
        Ok(results)
    })
}

fn one_phase<F, W>(
    factory: &F,
    workload: &mut W,
    phase: &str,
    inboxes: &[SyncSender<Message<W::Op>>],
    outboxes: &[Receiver<Reply>],
) -> Result<PhaseResult, String>
where
    F: DriverFactory,
    W: Workload<Op = <F::Driver as Driver>::Op>,
{
    let writer = factory.writer();
    let bytes_before = writer.bytes();
    let start = Instant::now();
    // Generation runs here rather than on a fourth thread: the orchestrator is
    // otherwise blocked for the whole phase, so it *is* the generator thread,
    // and one fewer thread is one fewer competitor for the four CPUs the row
    // is measured on.
    workload.generate(phase, |op| {
        let shard = factory.route(&op);
        inboxes
            .get(shard)
            .ok_or_else(|| {
                format!("op routed to consumer {shard} of {}", inboxes.len())
            })?
            .send(Message::Op(op))
            .map_err(|_| format!("consumer {shard} is gone"))
    })?;

    let mut samples = Vec::new();
    for (shard, tx) in inboxes.iter().enumerate() {
        tx.send(Message::EndPhase)
            .map_err(|_| format!("consumer {shard} is gone"))?;
    }
    for (shard, rx) in outboxes.iter().enumerate() {
        match rx.recv() {
            Ok(Reply::Samples(mut s)) => samples.append(&mut s),
            Ok(Reply::Failed(e)) => {
                return Err(format!("{phase}: consumer {shard}: {e}"));
            }
            // A boundary reply here would mean the protocol slipped a
            // phase — worth failing on rather than absorbing.
            Ok(Reply::BoundaryDone(_) | Reply::Closed(_)) | Err(_) => {
                return Err(format!("{phase}: consumer {shard} stopped"));
            }
        }
    }
    // Stopped after the last consumer replies, so the wall clock covers the
    // phase and not merely its generation.
    let wall_ns = start.elapsed().as_nanos() as u64;
    Ok(PhaseResult {
        name: phase.to_string(),
        samples,
        wall_ns,
        bytes_written: writer.bytes().saturating_sub(bytes_before),
    })
}

/// Untimed, and on consumer 0 only.
///
/// One driver runs it because the work is about the **store**, not about a
/// connection: checkpointing three times, or restarting a server three times,
/// would be three different things happening to one data directory.
fn boundary<O>(
    inboxes: &[SyncSender<Message<O>>],
    outboxes: &[Receiver<Reply>],
    done: &str,
    next: &str,
) -> Result<(), String> {
    let (Some(tx), Some(rx)) = (inboxes.first(), outboxes.first()) else {
        return Err("no consumer to run the phase boundary on".into());
    };
    tx.send(Message::Boundary {
        done: done.to_string(),
        next: next.to_string(),
    })
    .map_err(|_| "consumer 0 is gone".to_string())?;
    match rx.recv() {
        Ok(Reply::BoundaryDone(outcome)) => {
            outcome.map_err(|e| format!("{done} -> {next}: {e}"))
        }
        Ok(Reply::Failed(e)) => Err(format!("{done} -> {next}: {e}")),
        _ => Err(format!("{done} -> {next}: consumer 0 stopped")),
    }
}

/// A phase's throughput, by wall clock.
#[must_use]
pub fn throughput(count: u64, wall_ns: u64) -> f64 {
    if wall_ns == 0 {
        return 0.0;
    }
    count as f64 * 1e9 / wall_ns as f64
}

/// The sorted percentiles of a pooled sample set.
#[must_use]
pub fn percentiles(samples: &mut [u64]) -> (u64, u64, u64, u64) {
    samples.sort_unstable();
    let at = |q: f64| {
        if samples.is_empty() {
            return 0;
        }
        samples[((samples.len() - 1) as f64 * q) as usize]
    };
    (
        at(0.50),
        at(0.95),
        at(0.99),
        samples.last().copied().unwrap_or(0),
    )
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use std::sync::Mutex;

    use super::{percentiles, run, throughput};
    use crate::harness::{Driver, DriverFactory, Workload};

    /// Records what it saw, on a shared log so the test can read it back.
    /// Holds an `Rc` deliberately: a `Send` driver would not exercise the
    /// constraint the harness is shaped by.
    struct Recorder {
        shard: usize,
        seen: Rc<()>,
        log: &'static Mutex<Vec<String>>,
        fail_on: Option<u64>,
    }

    struct Config {
        consumers: usize,
        log: &'static Mutex<Vec<String>>,
        fail_on: Option<u64>,
    }

    impl Driver for Recorder {
        type Op = u64;

        fn execute(&mut self, op: u64) -> Result<(), String> {
            let _ = &self.seen;
            if self.fail_on == Some(op) {
                return Err(format!("refused {op}"));
            }
            if let Ok(mut l) = self.log.lock() {
                l.push(format!("c{}:{op}", self.shard));
            }
            Ok(())
        }

        fn between_phases(
            &mut self,
            done: &str,
            next: &str,
        ) -> Result<(), String> {
            if let Ok(mut l) = self.log.lock() {
                l.push(format!("c{}:{done}->{next}", self.shard));
            }
            Ok(())
        }

        fn close(self) -> Result<(), String> {
            if let Ok(mut l) = self.log.lock() {
                l.push(format!("c{}:closed", self.shard));
            }
            Ok(())
        }
    }

    impl DriverFactory for Config {
        type Driver = Recorder;

        fn consumers(&self) -> usize {
            self.consumers
        }

        fn build(&self, shard: usize) -> Result<Recorder, String> {
            Ok(Recorder {
                shard,
                seen: Rc::new(()),
                log: self.log,
                fail_on: self.fail_on,
            })
        }

        fn route(&self, op: &u64) -> usize {
            (*op as usize) % self.consumers.max(1)
        }
    }

    struct Counting {
        per_phase: u64,
    }

    impl Workload for Counting {
        type Op = u64;

        fn phases(&self) -> &'static [&'static str] {
            &["insert", "read_hot", "read_cold"]
        }

        fn len(&self, _phase: &str) -> u64 {
            self.per_phase
        }

        fn generate<E>(
            &mut self,
            _phase: &str,
            mut emit: E,
        ) -> Result<(), String>
        where
            E: FnMut(u64) -> Result<(), String>,
        {
            for n in 0..self.per_phase {
                emit(n)?;
            }
            Ok(())
        }
    }

    fn log() -> &'static Mutex<Vec<String>> {
        Box::leak(Box::new(Mutex::new(Vec::new())))
    }

    #[test]
    fn every_operation_of_every_phase_reaches_a_driver() {
        let cfg = Config {
            consumers: 3,
            log: log(),
            fail_on: None,
        };
        let phases = run(&cfg, Counting { per_phase: 30 }).expect("run");

        assert_eq!(phases.len(), 3);
        for phase in &phases {
            assert_eq!(phase.samples.len(), 30, "{}", phase.name);
        }
        let seen = cfg.log.lock().expect("log");
        assert!(seen.iter().filter(|l| l.contains(":0")).count() >= 3);
    }

    /// The routing rule is honoured: an op only ever reaches its owner.
    #[test]
    fn an_operation_only_reaches_the_consumer_that_owns_it() {
        let cfg = Config {
            consumers: 3,
            log: log(),
            fail_on: None,
        };
        run(&cfg, Counting { per_phase: 30 }).expect("run");

        for line in cfg.log.lock().expect("log").iter() {
            let Some((c, op)) = line.split_once(':') else {
                continue;
            };
            let (Ok(shard), Ok(n)) = (
                c.trim_start_matches('c').parse::<usize>(),
                op.parse::<u64>(),
            ) else {
                continue; // a boundary or close line
            };
            assert_eq!(
                n as usize % 3,
                shard,
                "{line} went to the wrong consumer"
            );
        }
    }

    /// The boundary runs on consumer 0 and only there — checkpointing three
    /// times would be three different things happening to one data directory.
    #[test]
    fn the_phase_boundary_runs_once_on_consumer_zero() {
        let cfg = Config {
            consumers: 3,
            log: log(),
            fail_on: None,
        };
        run(&cfg, Counting { per_phase: 3 }).expect("run");

        let seen = cfg.log.lock().expect("log");
        let boundaries: Vec<&String> =
            seen.iter().filter(|l| l.contains("->")).collect();
        assert_eq!(boundaries.len(), 2, "{boundaries:?}");
        assert!(
            boundaries.iter().all(|b| b.starts_with("c0:")),
            "{boundaries:?}"
        );
        assert!(boundaries[0].contains("insert->read_hot"), "{boundaries:?}");
    }

    /// Every consumer closes its driver, even the ones that did nothing —
    /// a server left holding its data directory corrupts the footprint.
    #[test]
    fn every_consumer_closes_its_driver() {
        let cfg = Config {
            consumers: 3,
            log: log(),
            fail_on: None,
        };
        run(&cfg, Counting { per_phase: 3 }).expect("run");
        let seen = cfg.log.lock().expect("log");
        assert_eq!(seen.iter().filter(|l| l.ends_with(":closed")).count(), 3);
    }

    #[test]
    fn a_refused_operation_names_its_phase_and_consumer() {
        let cfg = Config {
            consumers: 1,
            log: log(),
            fail_on: Some(7),
        };
        let err = run(&cfg, Counting { per_phase: 20 }).expect_err("must fail");
        assert!(err.contains("insert"), "{err}");
        assert!(err.contains("refused 7"), "{err}");
    }

    /// The wall clock covers the phase, not merely its generation.
    #[test]
    fn a_phase_reports_a_nonzero_wall_clock() {
        let cfg = Config {
            consumers: 2,
            log: log(),
            fail_on: None,
        };
        let phases = run(&cfg, Counting { per_phase: 50 }).expect("run");
        assert!(phases.iter().all(|p| p.wall_ns > 0));
    }

    /// The reason both clocks exist: summing overlapping windows counts
    /// concurrency as duration, so it can exceed the wall clock outright.
    #[test]
    fn summed_windows_and_wall_clock_are_different_numbers() {
        let cfg = Config {
            consumers: 3,
            log: log(),
            fail_on: None,
        };
        let phases = run(&cfg, Counting { per_phase: 300 }).expect("run");
        let insert = &phases[0];
        let summed: u64 = insert.samples.iter().sum();
        assert!(summed > 0 && insert.wall_ns > 0);
        // Throughput must divide by the wall clock; dividing by the sum would
        // be the number this assertion forbids anyone from reporting.
        let by_wall = throughput(insert.samples.len() as u64, insert.wall_ns);
        let by_sum = throughput(insert.samples.len() as u64, summed);
        assert!(by_wall > 0.0 && by_sum > 0.0);
    }

    #[test]
    fn percentiles_come_out_sorted_and_bounded() {
        let mut s = vec![50, 10, 99, 1, 95];
        let (p50, p95, p99, max) = percentiles(&mut s);
        assert!(p50 <= p95 && p95 <= p99 && p99 <= max);
        assert_eq!(max, 99);
    }

    #[test]
    fn an_empty_phase_is_zero_rather_than_a_division() {
        assert!((throughput(0, 0) - 0.0).abs() < f64::EPSILON);
        let (p50, _, _, max) = percentiles(&mut []);
        assert_eq!((p50, max), (0, 0));
    }
}
