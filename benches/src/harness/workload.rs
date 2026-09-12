//! What the generator thread produces.
//!
//! ## Streaming, not a `Vec`
//!
//! [`Workload::generate`] hands each operation to `emit` instead of returning
//! a collection, and the bounded channel behind `emit` is what makes the
//! generator run *concurrently with* its consumers rather than ahead of them.
//!
//! Returning a `Vec` would be simpler and wrong at the sizes this suite
//! exists to reach: a `large`-tier insert phase is five million records, and
//! materialising them is hundreds of megabytes inside a 500 MB cage that also
//! holds the engine. The generator would stop being a producer and become a
//! memory experiment.
//!
//! ## Why the phase is a parameter
//!
//! `read_hot` and `read_cold` are the same operation over the same key space
//! and differ only in what happened between them, so an op cannot say which
//! phase it belongs to — the phase has to be told. It is also what lets one
//! workload serve every system: the generator emits `read_cold` ops whether
//! the driver made anything cold or not, and making it cold is
//! [`Driver::between_phases`](super::Driver::between_phases)' job.

/// A deterministic source of operations, one phase at a time.
///
/// `Send` because it moves to the generator thread. It is the *only* thing
/// that does: the drivers stay where they were built.
pub trait Workload: Send {
    type Op;

    /// The phases this workload runs, in order.
    fn phases(&self) -> &'static [&'static str];

    /// How many operations `phase` will emit.
    ///
    /// Declared rather than counted, because the count is needed **before**
    /// the phase runs — to size the sample buffers — and counting would mean
    /// generating twice.
    fn len(&self, phase: &str) -> u64;

    /// Produce `phase`'s operations, handing each to `emit` in order.
    ///
    /// `emit` returns `Err` when the consumers are gone, which is how a
    /// generator learns to stop: a failed row must not leave a thread
    /// cheerfully building five million records nobody will execute.
    ///
    /// # Errors
    /// `emit` refusing, or a workload that cannot build its data.
    fn generate<E>(&mut self, phase: &str, emit: E) -> Result<(), String>
    where
        E: FnMut(Self::Op) -> Result<(), String>;
}
