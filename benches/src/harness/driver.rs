//! The seam every system implements ([RFC 0065] §3).
//!
//! A [`Driver`] is one connection to one database, living on one consumer
//! thread, that knows how to execute a workload operation and nothing else. It
//! does not generate, it does not time, and it does not know how many siblings
//! it has.
//!
//! ## Why a factory, and not just a driver
//!
//! The driver has to be **built on the thread that will use it**, so what
//! crosses the thread boundary is a [`DriverFactory`] — plain configuration,
//! `Send` — and never the driver itself. That is not a style preference: the
//! WaveDB engine is a current-thread `LocalSet` model whose futures are
//! deliberately non-`Send`, and a `ShardStore` holds an `Rc`, so constructing
//! one anywhere it could be moved from would not compile. It is the same split
//! `wavedb-quick-node/src/shard/worker.rs` makes for the node, for the same
//! reason.
//!
//! It also buys the thing the sharded row needs: each consumer calls
//! [`build`](DriverFactory::build) with its own index, so each gets its own
//! cache in front of one shared disk actor.
//!
//! ## No `dyn`
//!
//! Both traits are used through generics. The project's dispatch rule is a
//! compile-time `match` to monomorphised arms, and a benchmark measuring its
//! own vtable lookups would be a benchmark measuring the harness.
//!
//! ## Why phase boundaries are part of the trait
//!
//! Every system defers work, and every system is given a chance to finish it
//! **outside** a timed window: SQLite runs `PRAGMA wal_checkpoint(TRUNCATE)`,
//! WaveDB checkpoints and evicts, MongoDB restarts to empty the WiredTiger
//! cache, the SQL pair reopens a connection so the read that follows is cold.
//! Those are not incidental — they are what makes `read_cold` mean the same
//! thing on all five. Leaving them inside each adapter's `run` function is
//! what RFC 0060 did, and it is why they could drift; here they are a method
//! with a name, called at one place, for every system.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

/// One database connection, on one consumer thread.
pub trait Driver {
    /// The workload this driver speaks — [`MicroOp`](crate::plan::op::MicroOp)
    /// or [`ShopOp`](crate::plan::op::ShopOp).
    type Op;

    /// Execute one operation. **This call is the timed window**, so it must
    /// contain the database work and nothing else: no generation, no
    /// bookkeeping, no assertion that reads back more than the operation
    /// already did.
    ///
    /// # Errors
    /// Whatever the database said. A failing operation ends its row rather
    /// than being counted — a rate computed over operations that did not
    /// happen is not a rate.
    fn execute(&mut self, op: Self::Op) -> Result<(), String>;

    /// Untimed work after each operation, on the consumer that ran it.
    ///
    /// The counterpart of a server.s background threads, and the reason it is
    /// on the trait rather than inside `execute`: PostgreSQL.s WAL writer,
    /// MySQL.s page cleaners and WiredTiger.s eviction all run *concurrently
    /// with* a timed window on CPUs the client is not using, so an embedded
    /// engine that did its equivalent *inside* the window would be charged for
    /// work its peers hide. WaveDB checkpoints and evicts here, above a
    /// journal-byte threshold; everyone else does nothing.
    ///
    /// It is a byte threshold and not a count, and that distinction has
    /// already cost this project once: RFC 0060.s adapter used
    /// `MAINTAIN_EVERY = 5000` operations, which never fired while 649 MB of
    /// journal accumulated, because a count of operations says nothing about
    /// how much log they produced.
    ///
    /// # Errors
    /// A checkpoint that failed. The row stops rather than carrying on with
    /// an engine whose maintenance is behind.
    fn after_op(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Untimed work between two phases, on **consumer 0 only**.
    ///
    /// Called after every consumer has drained `done` and before any has
    /// started `next`, so it may reopen connections, checkpoint, or restart a
    /// server without racing a sibling. The default does nothing, which is
    /// right for a system that defers nothing between phases.
    ///
    /// # Errors
    /// A checkpoint or reopen that failed. The row stops: a `read_cold` phase
    /// whose cold-making step silently failed would report a hot number under
    /// a cold name.
    fn between_phases(
        &mut self,
        _done: &str,
        _next: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    /// Release the connection. Called once per consumer, after the last
    /// phase, and **before** any footprint is measured — a data directory read
    /// while a server still holds it measures that server's deferral, not its
    /// storage.
    ///
    /// # Errors
    /// A close that failed, which for a server means it may not have
    /// checkpointed.
    fn close(self) -> Result<(), String>
    where
        Self: Sized,
    {
        Ok(())
    }
}

/// Builds one [`Driver`] per consumer thread.
///
/// `Send`, because this is the half that crosses; the driver it makes is not
/// required to be.
pub trait DriverFactory: Send {
    type Driver: Driver;

    /// How many consumers this configuration wants.
    ///
    /// Asked of the factory rather than passed in, because only the adapter
    /// knows what it can honour: everything but sharded WaveDB on the `shop`
    /// workload answers 1, and answering more would not be a faster
    /// measurement but a wrong one (see
    /// [`MicroOp::partition_key`](crate::plan::op::MicroOp::partition_key)).
    fn consumers(&self) -> usize;

    /// Build the driver for consumer `shard`, on that consumer's own thread.
    ///
    /// # Errors
    /// The connection, or the store, could not be opened.
    fn build(&self, shard: usize) -> Result<Self::Driver, String>;

    /// Which consumer owns `op`.
    ///
    /// The routing rule is the adapter's because the ownership rule is:
    /// sharded WaveDB routes by `Owner::Collection(pivot)` — the Pivot
    /// instance being [RFC 0064]'s unit of concurrency — while a
    /// single-consumer system routes everything to 0. A shared queue would be
    /// neither.
    ///
    /// [RFC 0064]: ../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md
    fn route(&self, op: &<Self::Driver as Driver>::Op) -> usize;

    /// Whose disk writes this row's phases should be attributed to.
    ///
    /// The embedded bracket writes in this process; the server bracket writes
    /// in the server's, and reading `self` for a server row would report a
    /// flat zero for every phase — a wrong number rather than a missing one.
    /// The default is the embedded answer, which is also the one every
    /// single-process driver wants.
    fn writer(&self) -> crate::metrics::Writer {
        crate::metrics::Writer::Current
    }
}

/// What crosses a thread must be `Send`; what a consumer holds must not have
/// to be.
///
/// Structural rather than asserted in prose, and the *second* half is the one
/// worth pinning: a driver holding an `Rc` is the whole reason the factory
/// exists, so nothing here may quietly acquire a `Send` bound on `Driver`.
#[cfg(test)]
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<tests::Config>();
};

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::{Driver, DriverFactory};

    /// Deliberately holds an `Rc`: a driver that could be `Send` would not
    /// exercise the constraint this seam is shaped by.
    pub(super) struct Fake {
        shard: usize,
        log: Rc<std::cell::RefCell<Vec<String>>>,
    }

    pub(super) struct Config {
        pub consumers: usize,
    }

    impl Driver for Fake {
        type Op = u64;

        fn execute(&mut self, op: u64) -> Result<(), String> {
            if op == u64::MAX {
                return Err("refused".into());
            }
            self.log.borrow_mut().push(format!("{}:{op}", self.shard));
            Ok(())
        }

        fn between_phases(
            &mut self,
            done: &str,
            next: &str,
        ) -> Result<(), String> {
            self.log.borrow_mut().push(format!("{done}->{next}"));
            Ok(())
        }
    }

    impl DriverFactory for Config {
        type Driver = Fake;

        fn consumers(&self) -> usize {
            self.consumers
        }

        fn build(&self, shard: usize) -> Result<Fake, String> {
            Ok(Fake {
                shard,
                log: Rc::new(std::cell::RefCell::new(Vec::new())),
            })
        }

        fn route(&self, op: &u64) -> usize {
            (*op as usize) % self.consumers.max(1)
        }
    }

    /// The seam is used through generics, never `dyn` — this function is the
    /// proof, since a `dyn Driver` would not compile against a trait with
    /// `close(self)` and an associated type used this way.
    fn drive<F: DriverFactory>(
        factory: &F,
        ops: Vec<<F::Driver as Driver>::Op>,
    ) -> Result<usize, String> {
        let mut driver = factory.build(0)?;
        let mut n = 0;
        for op in ops {
            driver.execute(op)?;
            n += 1;
        }
        driver.close()?;
        Ok(n)
    }

    #[test]
    fn a_factory_drives_its_own_driver_through_generics() {
        let cfg = Config { consumers: 1 };
        assert_eq!(drive(&cfg, vec![1, 2, 3]).expect("drive"), 3);
    }

    /// A failing operation stops the row rather than being counted: a rate
    /// computed over operations that did not happen is not a rate.
    #[test]
    fn a_refused_operation_ends_the_run() {
        let cfg = Config { consumers: 1 };
        let err = drive(&cfg, vec![1, u64::MAX, 3]).expect_err("must stop");
        assert_eq!(err, "refused");
    }

    /// Routing is the adapter's, and it must cover exactly the consumers it
    /// asked for — an op routed past the last consumer has nowhere to go.
    #[test]
    fn every_op_routes_inside_the_consumer_count() {
        let cfg = Config { consumers: 3 };
        for op in 0..100u64 {
            assert!(cfg.route(&op) < cfg.consumers());
        }
    }

    #[test]
    fn a_single_consumer_factory_routes_everything_to_zero() {
        let cfg = Config { consumers: 1 };
        for op in 0..100u64 {
            assert_eq!(cfg.route(&op), 0);
        }
    }

    /// The default hook does nothing, so a system that defers nothing between
    /// phases implements nothing.
    #[test]
    fn the_phase_hook_is_optional() {
        struct Bare;
        impl Driver for Bare {
            type Op = ();
            fn execute(&mut self, (): ()) -> Result<(), String> {
                Ok(())
            }
        }
        assert!(Bare.between_phases("insert", "read_hot").is_ok());
    }

    #[test]
    fn the_phase_hook_sees_both_names() {
        let cfg = Config { consumers: 1 };
        let mut d = cfg.build(0).expect("build");
        d.between_phases("insert", "read_hot").expect("hook");
        assert_eq!(d.log.borrow().as_slice(), ["insert->read_hot"]);
    }
}
