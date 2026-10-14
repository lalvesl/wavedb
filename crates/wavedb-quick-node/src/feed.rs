//! The mutation feed: committed mutations from the shards to the thread that
//! owns the subscription table ([RFC 0066] §2).
//!
//! The shards execute writes on their own threads and cannot touch the
//! table, so each hands its [`Mutation`] over as plain data and `publish`
//! — on the accept thread — does the delivery.
//!
//! ## Full policy: a write never waits on an observer
//!
//! The queue is bounded, and a shard that finds it full **drops** the
//! mutation instead of waiting: a write's latency must not depend on how fast
//! WebSocket clients read. Dropping is safe because sync is stateless (RFC
//! 0034): the publisher is told a drop happened, disconnects every
//! subscriber, and each one reconnects and catches up by cursor.
//!
//! The publisher cannot tell which topics the dropped event belonged to, so
//! it disconnects all of them. That is coarse on purpose: a full feed means
//! the publisher is [`MUTATION_DEPTH`] events behind, which is an overload,
//! and a precise per-topic record would need a structure the shards share.
//!
//! **Ordering.** A shard raises the lag signal *before* it pushes anything
//! newer, and the publisher checks the signal *after* each pop and before
//! delivering. So any event the publisher sees from after a drop finds the
//! signal already raised, and a subscriber can never receive a later event of
//! a topic without the earlier one — which would move its cursor past the gap.
//!
//! [RFC 0066]: ../../../rfcs/0066-embassy-executor-and-async-io.md

use std::cell::RefCell;
use std::rc::Rc;

use async_channel::{Receiver, Sender, TrySendError};
use embassy_futures::select::{Either, select};
use wavedb_core::notify::Mutation;

use crate::subscribe::{Publish, SubTable};

/// Mutations in flight between the shards and the publisher before a shard
/// starts dropping them.
///
/// One queue for the whole node, fed by every shard. Deep, because each slot
/// that overflows costs every subscriber a reconnect; bounded, because the
/// alternative is the publisher's lag becoming the node's memory.
pub const MUTATION_DEPTH: usize = 4096;

/// The shards' end: cheap to clone, `Send`, never blocks.
#[derive(Clone)]
pub struct MutationFeed {
    events: Sender<Mutation>,
    /// Capacity one: a raised signal is all the publisher needs, however
    /// many mutations were dropped before it looked.
    lagged: Sender<()>,
}

/// The publisher's end.
pub struct FeedReceiver {
    events: Receiver<Mutation>,
    lagged: Receiver<()>,
}

/// A feed holding at most `depth` undelivered mutations.
#[must_use]
pub fn feed(depth: usize) -> (MutationFeed, FeedReceiver) {
    let (events, events_rx) = async_channel::bounded(depth);
    let (lagged, lagged_rx) = async_channel::bounded(1);
    (
        MutationFeed { events, lagged },
        FeedReceiver {
            events: events_rx,
            lagged: lagged_rx,
        },
    )
}

impl Publish for MutationFeed {
    fn publish(&self, mutation: &Mutation) {
        match self.events.try_send(mutation.clone()) {
            // A signal already pending covers this drop too.
            Err(TrySendError::Full(_)) => {
                let _ = self.lagged.try_send(());
            }
            // Closed: the node is shutting down, nobody is left to deliver to.
            Ok(()) | Err(TrySendError::Closed(_)) => {}
        }
    }
}

/// Deliver mutations to the subscribers until every shard's feed is dropped.
pub(crate) async fn publish(feed: FeedReceiver, subs: Rc<RefCell<SubTable>>) {
    loop {
        match select(feed.events.recv(), feed.lagged.recv()).await {
            Either::First(Ok(mutation)) => {
                if feed.lagged.try_recv().is_ok() {
                    subs.borrow_mut().disconnect_all();
                }
                subs.publish(&mutation);
            }
            Either::Second(Ok(())) => subs.borrow_mut().disconnect_all(),
            // Both ends close together, when the last shard drops its feed.
            Either::First(Err(_)) | Either::Second(Err(_)) => break,
        }
    }
    // Anything the shards pushed before closing still goes out.
    while let Ok(mutation) = feed.events.try_recv() {
        subs.publish(&mutation);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use wavedb_core::notify::{Mutation, MutationKind};
    use wavedb_core::{Id, U48};
    use wavedb_net::ws::{ServerMsg, Topic};

    use super::{feed, publish};
    use crate::subscribe::{Publish, SubTable};

    fn mutation(n: u64) -> Mutation {
        let tenant = U48::from(1u32);
        Mutation {
            struct_hash: 0xABCD,
            tenant,
            pivot: None,
            id: Id::new(n, tenant, false, 1),
            kind: MutationKind::Saved,
            meta: None,
            body: Vec::new(),
        }
    }

    fn subscribed()
    -> (Rc<RefCell<SubTable>>, async_channel::Receiver<ServerMsg>) {
        let subs = Rc::new(RefCell::new(SubTable::default()));
        let (tx, rx) = async_channel::bounded(16);
        let conn = subs.borrow_mut().new_conn();
        let topic = Topic {
            struct_hash: 0xABCD,
            pivot: None,
        };
        subs.borrow_mut()
            .subscribe(U48::from(1u32), topic, conn, tx);
        (subs, rx)
    }

    /// The write side's policy: publishing into a full feed returns at once.
    /// `publish` is synchronous, so "never blocks" is the type's promise; what
    /// this pins is that the overflow is *signalled*, not lost silently.
    #[test]
    fn a_full_feed_drops_and_signals_instead_of_waiting() {
        let (tx, rx) = feed(1);
        tx.publish(&mutation(1));
        tx.publish(&mutation(2)); // full: dropped
        tx.publish(&mutation(3)); // full: dropped, signal already raised
        assert_eq!(rx.events.len(), 1, "the feed holds only its depth");
        assert!(rx.lagged.try_recv().is_ok(), "the drop is signalled");
        assert!(rx.lagged.try_recv().is_err(), "once is enough");
    }

    /// The read side's policy: once a mutation was dropped, every subscriber
    /// is disconnected to catch up by cursor, rather than left watching a
    /// stream with a hole in it.
    #[test]
    fn a_lagging_publisher_disconnects_the_subscribers() {
        let (subs, events) = subscribed();
        let (tx, rx) = feed(1);
        tx.publish(&mutation(1));
        tx.publish(&mutation(2)); // dropped
        drop(tx);

        futures::executor::block_on(publish(rx, Rc::clone(&subs)));

        assert!(events.is_closed(), "the subscriber was disconnected");
        assert!(subs.borrow().is_empty());
    }

    /// Without a drop the feed is a plain queue: everything arrives, in order.
    #[test]
    fn an_unlagged_feed_delivers_everything_in_order() {
        let (subs, events) = subscribed();
        let (tx, rx) = feed(8);
        for n in 1..=3 {
            tx.publish(&mutation(n));
        }
        drop(tx);

        futures::executor::block_on(publish(rx, Rc::clone(&subs)));

        for n in 1..=3 {
            let Ok(ServerMsg::Event(event)) = events.try_recv() else {
                panic!("event {n} missing");
            };
            assert_eq!(event.id.key(), n);
        }
        assert!(!events.is_closed(), "nobody was disconnected");
    }
}
