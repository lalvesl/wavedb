//! Declared subscriptions and the [`NotifyStore`] that feeds them — the
//! node half of M7 live sync.
//!
//! A WebSocket connection [`Subscribe`](wavedb_net::ws::ClientMsg::Subscribe)s
//! to a [`Topic`] (a Unique anchor or one collection `Pivot`). Every
//! committed mutation crosses [`Store::note_mutation`]; [`NotifyStore`]
//! wraps the engine store, overrides that hook, and routes the event to
//! exactly the connections subscribed to its `(tenant, topic)` key —
//! O(subscribers), an exact-match lookup, never a scan of the data. No
//! `dyn`: the wrapper is one concrete type the node's serve path names.
//!
//! ## Where the table lives: with the topic's owner
//!
//! A topic **is** an owner — a Pivot, or a Unique type under a tenant — so
//! its mutations are committed on one worker only, the one that owns it
//! ([RFC 0067] §6). Each worker keeps the subscriptions to the topics it
//! owns, and a commit publishes straight into each subscriber's outbox: an
//! `async-channel` whose receiving end is the session, on whichever thread
//! accepted that connection. Nothing sits between the commit and the outbox
//! — no node-wide queue, no publisher task — and a session subscribes by
//! sending its outbox to the owner's worker ([`Router::watch`]).
//!
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md
//! [`Router::watch`]: crate::shard::Router::watch

use std::cell::RefCell;
use std::collections::HashMap;

use async_channel::{Sender, TrySendError};
use wavedb_core::notify::{Mutation, MutationKind};
use wavedb_core::{Id, Store, U48, Write};
use wavedb_net::ws::{EventKind, RecordEvent, ServerMsg, Topic};

/// The routing key: a topic scoped to the tenant whose data it is. The
/// tenant is the subscriber connection's bound identity, never a client
/// field — a connection can only ever watch its own tenant.
type Key = (U48, Topic);

/// A session's outbox: what it drains, and how a subscription names the
/// session that holds it — two outboxes are one session exactly when they
/// are one channel ([`Sender::same_channel`]), so no id has to be minted
/// across threads.
pub type Outbox = Sender<ServerMsg>;

/// The subscriptions to the topics one worker owns. Its jobs and its root
/// run on one thread, so a `RefCell` is all the sharing it needs.
#[derive(Default)]
pub struct SubTable {
    by_topic: HashMap<Key, Vec<Outbox>>,
}

impl SubTable {
    /// Register `outbox`'s interest in `topic` under `tenant`. The session
    /// guarantees one call per `(outbox, topic)` (it tracks its own set).
    pub fn subscribe(&mut self, tenant: U48, topic: Topic, outbox: Outbox) {
        self.by_topic
            .entry((tenant, topic))
            .or_default()
            .push(outbox);
    }

    /// No subscription is registered.
    #[cfg(test)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_topic.is_empty()
    }

    /// Drop `outbox`'s interest in `topic`.
    pub fn unsubscribe(&mut self, tenant: U48, topic: Topic, outbox: &Outbox) {
        let key = (tenant, topic);
        if let Some(subs) = self.by_topic.get_mut(&key) {
            subs.retain(|s| !s.same_channel(outbox));
            if subs.is_empty() {
                self.by_topic.remove(&key);
            }
        }
    }

    /// Push one mutation to the connections watching its topic.
    ///
    /// A closed outbox (its session ended between `apply` and here) is pruned.
    /// A **full** one is a client reading slower than its topics change: it
    /// is disconnected rather than buffered without limit. Its session drains
    /// what is queued and ends, and the client reconnects and catches up from
    /// its cursor (RFC 0034) — sync is stateless, so nothing is lost.
    fn publish(&mut self, m: &Mutation) {
        let topic = Topic {
            struct_hash: m.struct_hash,
            pivot: m.pivot,
        };
        let key = (m.tenant, topic);
        let Some(subs) = self.by_topic.get_mut(&key) else {
            return;
        };
        let event = ServerMsg::Event(RecordEvent {
            topic,
            id: m.id,
            kind: match m.kind {
                MutationKind::Saved => EventKind::Saved,
                MutationKind::Removed(at) => EventKind::Removed(at),
            },
            meta: m.meta.clone(),
            body: m.body.clone(),
        });
        subs.retain(|s| match s.try_send(event.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                s.close();
                false
            }
            Err(TrySendError::Closed(_)) => false,
        });
        if subs.is_empty() {
            self.by_topic.remove(&key);
        }
    }
}

/// The engine store wrapped so committed mutations reach subscribers.
///
/// Forwards every [`Store`] read/write to the inner store (on the node, a
/// worker's view of one data shard, built per operation) and overrides
/// [`note_mutation`](Store::note_mutation) to push to the WebSocket
/// subscribers of the worker that owns the topic — the one running this
/// store. (HTTP poll-watches need no publish half: each sync navigates the
/// disk from the client's cursor.) The node serves **through** this
/// wrapper, so a mutation driven by any transport — a POST command, a
/// WebSocket `Call`, or a `#[server]` body — reaches the live watchers;
/// node-side seeding (which touches the raw engine before serving) does not,
/// and neither do cache mirrors (a different store).
pub struct NotifyStore<'a, S> {
    inner: S,
    subs: &'a RefCell<SubTable>,
}

impl<'a, S> NotifyStore<'a, S> {
    /// Wrap a store, publishing mutations into `subs`.
    pub const fn new(inner: S, subs: &'a RefCell<SubTable>) -> Self {
        Self { inner, subs }
    }
}

impl<S: Store> Store for NotifyStore<'_, S> {
    async fn get(&self, id: Id) -> wavedb_core::Result<Option<Vec<u8>>> {
        self.inner.get(id).await
    }

    async fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> wavedb_core::Result<Option<Vec<u8>>> {
        self.inner.get_of(struct_hash, id).await
    }

    async fn apply(&self, batch: &[Write]) -> wavedb_core::Result<()> {
        self.inner.apply(batch).await
    }

    fn note_mutation(&self, mutation: impl FnOnce() -> Mutation) {
        // Building the event costs a body clone; only the node pays it (an
        // ordinary store's default drops the closure unbuilt), and only on
        // writes. The publish is exact-match routing — no data scan.
        let mutation = mutation();
        self.subs.borrow_mut().publish(&mutation);
    }
}

/// What crosses a thread for a subscription: the outbox, and the event the
/// owner pushes into it. The table itself never does.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<Outbox>();
    assert_send::<ServerMsg>();
};

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use async_channel::bounded;
    use wavedb_core::notify::{Mutation, MutationKind};
    use wavedb_core::{Id, LocalId, Store, U48, Write};
    use wavedb_net::ws::{EventKind, ServerMsg, Topic};

    use super::{NotifyStore, SubTable};

    /// Room for every event a test publishes, so only the test that means to
    /// fill an outbox does.
    const ROOMY: usize = 8;

    /// A store that records nothing — the inner engine stand-in.
    #[derive(Default)]
    struct NullStore;
    // A test double, written in the trait's own (async) shape.
    #[allow(clippy::unused_async_trait_impl)]
    impl Store for NullStore {
        async fn get(&self, _: Id) -> wavedb_core::Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn apply(&self, _: &[Write]) -> wavedb_core::Result<()> {
            Ok(())
        }
    }

    fn mutation(tenant: U48, pivot: Option<LocalId>) -> Mutation {
        Mutation {
            struct_hash: 0xABCD,
            tenant,
            pivot,
            id: Id::new(7, tenant, false, 1),
            kind: MutationKind::Saved,
            meta: None,
            body: vec![1, 2, 3],
        }
    }

    fn unique_topic() -> Topic {
        Topic {
            struct_hash: 0xABCD,
            pivot: None,
        }
    }

    #[test]
    fn a_mutation_reaches_only_its_topics_subscribers() {
        let tenant = U48::from(3u32);
        let subs = RefCell::new(SubTable::default());
        let (tx, rx) = bounded(ROOMY);
        let topic = unique_topic();
        subs.borrow_mut().subscribe(tenant, topic, tx);

        // A subscriber on a *different* pivot must not hear this.
        let (other_tx, other_rx) = bounded(ROOMY);
        subs.borrow_mut().subscribe(
            tenant,
            Topic {
                struct_hash: 0xABCD,
                pivot: Some(LocalId::new(9, true, 0)),
            },
            other_tx,
        );

        let store = NotifyStore::new(NullStore, &subs);
        store.note_mutation(|| mutation(tenant, None));

        let ServerMsg::Event(event) = rx.try_recv().unwrap() else {
            panic!("expected an event");
        };
        assert_eq!(event.topic, topic);
        assert_eq!(event.kind, EventKind::Saved);
        assert_eq!(event.body, vec![1, 2, 3]);
        assert!(other_rx.try_recv().is_err(), "wrong topic must stay silent");
    }

    #[test]
    fn another_tenants_mutation_is_never_delivered() {
        let subs = RefCell::new(SubTable::default());
        let (tx, rx) = bounded(ROOMY);
        subs.borrow_mut()
            .subscribe(U48::from(3u32), unique_topic(), tx);
        let store = NotifyStore::new(NullStore, &subs);
        // Same topic shape, foreign tenant — the key differs.
        store.note_mutation(|| mutation(U48::from(4u32), None));
        assert!(rx.try_recv().is_err());
    }

    /// A subscription is named by its outbox: unsubscribing one session
    /// leaves another on the same topic in place.
    #[test]
    fn unsubscribe_drops_only_that_outbox() {
        let tenant = U48::from(5u32);
        let topic = unique_topic();
        let subs = RefCell::new(SubTable::default());
        let (tx, rx) = bounded(ROOMY);
        let (kept_tx, kept_rx) = bounded(ROOMY);
        subs.borrow_mut().subscribe(tenant, topic, tx.clone());
        subs.borrow_mut().subscribe(tenant, topic, kept_tx);
        subs.borrow_mut().unsubscribe(tenant, topic, &tx);

        let store = NotifyStore::new(NullStore, &subs);
        store.note_mutation(|| mutation(tenant, None));
        assert!(rx.try_recv().is_err(), "unsubscribed outbox hears nothing");
        assert!(kept_rx.try_recv().is_ok(), "the other session still does");
    }

    #[test]
    fn the_last_unsubscribe_empties_the_table() {
        let tenant = U48::from(5u32);
        let subs = RefCell::new(SubTable::default());
        let (tx, _rx) = bounded(ROOMY);
        subs.borrow_mut()
            .subscribe(tenant, unique_topic(), tx.clone());
        subs.borrow_mut().unsubscribe(tenant, unique_topic(), &tx);
        assert!(subs.borrow().is_empty());
    }

    #[test]
    fn a_closed_receiver_is_pruned_on_publish() {
        let tenant = U48::from(6u32);
        let subs = RefCell::new(SubTable::default());
        let (tx, rx) = bounded(ROOMY);
        subs.borrow_mut().subscribe(tenant, unique_topic(), tx);
        drop(rx); // the session task ended without unregistering.

        let store = NotifyStore::new(NullStore, &subs);
        store.note_mutation(|| mutation(tenant, None));
        assert!(subs.borrow().is_empty(), "the dead subscription is dropped");
    }

    /// The outbound policy: a client reading slower than its topics change is
    /// disconnected, not buffered without limit. What was queued before the
    /// overflow is still delivered — the client's cursor stops there, and its
    /// catch-up after reconnecting starts from the event that did not fit.
    #[test]
    fn a_full_outbox_disconnects_the_slow_client() {
        let tenant = U48::from(7u32);
        let subs = RefCell::new(SubTable::default());
        let (tx, rx) = bounded(2);
        subs.borrow_mut().subscribe(tenant, unique_topic(), tx);

        let store = NotifyStore::new(NullStore, &subs);
        for _ in 0..3 {
            store.note_mutation(|| mutation(tenant, None));
        }

        assert!(rx.is_closed(), "the session must see its outbox close");
        assert!(matches!(rx.try_recv(), Ok(ServerMsg::Event(_))));
        assert!(matches!(rx.try_recv(), Ok(ServerMsg::Event(_))));
        assert!(
            rx.try_recv().is_err(),
            "the overflowing event is not queued"
        );
        assert!(subs.borrow().is_empty(), "and it is unsubscribed");
    }
}
