//! One consumer thread: a driver, a queue, and a stopwatch.
//!
//! The consumer is the **only** place an operation is timed, and it times
//! exactly [`Driver::execute`](super::Driver::execute) — not the receive, not
//! the routing, not the generation that produced the op on another thread.
//! That is the whole point of the split: under RFC 0060 generation ran
//! serially with the operation it fed, so the gap between two timed windows
//! was work the harness was doing, on the same CPU, while the database sat
//! idle.

use std::sync::mpsc::{Receiver, Sender};
use std::time::Instant;

use super::Driver;

/// What the orchestrator sends a consumer.
pub enum Message<O> {
    /// Execute this.
    Op(O),
    /// The phase is over: reply with its samples and wait for the next.
    EndPhase,
    /// Run the untimed work between two phases.
    ///
    /// Sent to consumer 0 only, and only after **every** consumer has
    /// replied to `EndPhase` — so a checkpoint or a reopen cannot race an
    /// operation still in flight on a sibling.
    Boundary { done: String, next: String },
    /// No more phases. Close the driver and exit.
    Shutdown,
}

/// What a consumer sends back.
pub enum Reply {
    /// One phase's per-operation samples, in nanoseconds.
    ///
    /// Sent unsorted and un-aggregated: the orchestrator pools every
    /// consumer's samples before computing percentiles, because a percentile
    /// of per-thread percentiles is not a percentile of anything.
    Samples(Vec<u64>),
    /// The driver refused, and the row is over.
    Failed(String),
    /// The between-phases hook ran (or did not).
    BoundaryDone(Result<(), String>),
    /// This consumer has closed its driver and is exiting.
    Closed(Result<(), String>),
}

/// Serve messages until told to stop.
///
/// Runs on the consumer's own thread, and owns its driver for the whole row —
/// a connection or a store that was rebuilt between phases would make
/// `read_hot` cold and `read_cold` meaningless.
pub fn serve<D: Driver>(
    mut driver: D,
    inbox: &Receiver<Message<D::Op>>,
    outbox: &Sender<Reply>,
    capacity: usize,
) {
    let mut samples = Vec::with_capacity(capacity);
    while let Ok(message) = inbox.recv() {
        match message {
            Message::Op(op) => {
                let start = Instant::now();
                let result = driver.execute(op);
                // Recorded before the error is examined: an operation that
                // failed still consumed the time it consumed, and dropping
                // the sample would flatter the phase it died in.
                samples.push(start.elapsed().as_nanos() as u64);
                if let Err(e) = result {
                    let _ = outbox.send(Reply::Failed(e));
                    return;
                }
                // Outside the window on purpose: this is the embedded
                // bracket.s answer to a server.s background threads.
                if let Err(e) = driver.after_op() {
                    let _ = outbox.send(Reply::Failed(e));
                    return;
                }
            }
            Message::EndPhase => {
                let _ =
                    outbox.send(Reply::Samples(std::mem::take(&mut samples)));
                samples.reserve(capacity);
            }
            Message::Boundary { done, next } => {
                let outcome = driver.between_phases(&done, &next);
                let _ = outbox.send(Reply::BoundaryDone(outcome));
            }
            Message::Shutdown => {
                let _ = outbox.send(Reply::Closed(driver.close()));
                return;
            }
        }
    }
    // The orchestrator dropped the sender without shutting down — a failure
    // elsewhere. Closing is still the right thing: a server left holding its
    // data directory would corrupt the footprint that follows.
    let _ = outbox.send(Reply::Closed(driver.close()));
}
