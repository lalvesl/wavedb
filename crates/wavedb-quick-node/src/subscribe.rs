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

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use async_channel::{Sender, TrySendError};
use wavedb_core::notify::{Mutation, MutationKind};
use wavedb_core::{Id, Store, U48, Write};
use wavedb_net::ws::{EventKind, RecordEvent, ServerMsg, Topic};

/// The routing key: a topic scoped to the tenant whose data it is. The
/// tenant is the subscriber connection's bound identity, never a client
/// field — a connection can only ever watch its own tenant.
type Key = (U48, Topic);

/// One connection's interest in a topic: its id (to unregister on
/// disconnect) and the channel its session task drains.
struct Sub {
    conn: u64,
    tx: Sender<ServerMsg>,
}

/// The live subscription table — one per node, shared (`Rc<RefCell>`) by
/// every connection task on the accept carrier (one thread, so no lock).
#[derive(Default)]
pub struct SubTable {
    by_topic: HashMap<Key, Vec<Sub>>,
    /// Monotonic connection id source — each WebSocket session takes one to
    /// tag (and later drop) its subscriptions.
    next_conn: u64,
}

impl SubTable {
    /// Mint a fresh connection id for a session.
    pub const fn new_conn(&mut self) -> u64 {
        let conn = self.next_conn;
        self.next_conn += 1;
        conn
    }

    /// Register `conn`'s interest in `topic` under `tenant`. The session
    /// guarantees one call per `(conn, topic)` (it tracks its own set).
    pub fn subscribe(
        &mut self,
        tenant: U48,
        topic: Topic,
        conn: u64,
        tx: Sender<ServerMsg>,
    ) {
        self.by_topic
            .entry((tenant, topic))
            .or_default()
            .push(Sub { conn, tx });
    }

    /// No subscription is registered.
    #[cfg(test)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_topic.is_empty()
    }

    /// Drop `conn`'s interest in `topic`.
    pub fn unsubscribe(&mut self, tenant: U48, topic: Topic, conn: u64) {
        let key = (tenant, topic);
        if let Some(subs) = self.by_topic.get_mut(&key) {
            subs.retain(|s| s.conn != conn);
            if subs.is_empty() {
                self.by_topic.remove(&key);
            }
        }
    }

    /// Disconnect every subscriber: close each outbox and empty the table.
    ///
    /// What a lost mutation costs (`crate::feed`): the publisher cannot tell
    /// which topics the dropped event belonged to, so every watcher reconnects
    /// and catches up by cursor. Each session sees its outbox close after the
    /// events already queued in it, so a client's cursor never passes the gap.
    pub fn disconnect_all(&mut self) {
        for sub in self.by_topic.values().flatten() {
            sub.tx.close();
        }
        self.by_topic.clear();
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
        subs.retain(|s| match s.tx.try_send(event.clone()) {
            Ok(()) => true,
            Err(TrySendError::Full(_)) => {
                s.tx.close();
                false
            }
            Err(TrySendError::Closed(_)) => false,
        });
        if subs.is_empty() {
            self.by_topic.remove(&key);
        }
    }
}

/// A shared handle to the node's subscription table — one per node, cloned
/// into the [`NotifyStore`] publisher and every WebSocket session.
pub type Subscriptions = Rc<RefCell<SubTable>>;

/// The engine store wrapped so committed mutations reach subscribers.
///
/// Forwards every [`Store`] read/write to the inner engine (a shared
/// `Rc<PageStore>` — the node's maintenance loop and final commit drive the
/// same engine directly) and overrides
/// [`note_mutation`](Store::note_mutation) to push to the WebSocket
/// subscribers. (HTTP poll-watches need no publish half: each sync
/// navigates the disk from the client's cursor.) The node serves
/// **through** this wrapper, so a mutation driven by any transport — a
/// POST command, a WebSocket `Call`, or a `#[server]` body — reaches the
/// live watchers; node-side seeding (which touches the raw engine before
/// serving) does not, and neither do cache mirrors (a different store).
pub struct NotifyStore<S, P = Subscriptions> {
    inner: Rc<S>,
    subs: P,
}

/// Where a committed mutation goes.
///
/// Two implementations, and the difference is **which thread owns the
/// subscription table**: [`Subscriptions`] when it is the caller's own, and
/// [`crate::feed::MutationFeed`] when another thread holds it. It is a trait
/// rather than an enum because an enum holding the local variant would make
/// every `NotifyStore` non-`Send`, including the ones that must cross — the
/// property is per-instance, so it belongs in the type. Monomorphised: no
/// `dyn`.
pub trait Publish {
    fn publish(&self, mutation: &Mutation);
}

/// Same thread as the table: fan out directly. Today's node, and the tests.
impl Publish for Subscriptions {
    fn publish(&self, mutation: &Mutation) {
        self.borrow_mut().publish(mutation);
    }
}

impl<S, P> NotifyStore<S, P> {
    /// Wrap a shared engine handle, publishing mutations through `subs`.
    pub const fn new(inner: Rc<S>, subs: P) -> Self {
        Self { inner, subs }
    }
}

impl<S: Store, P: Publish> Store for NotifyStore<S, P> {
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
        self.subs.publish(&mutation);
    }
}

/// What actually has to cross a thread for a shard to publish: the mutation
/// and the channel carrying it.
///
/// **Not** the `NotifyStore` itself — that is built on the shard's own thread
/// and holds an `Rc`, exactly like `ShardStore`, and must stay non-`Send` so
/// it cannot drift. An earlier version of this assertion asked for the wrapper
/// to be `Send` and the compiler refused, which was the right answer to the
/// wrong question: the seam is the *message*, not the store.
const _: fn() = || {
    const fn assert_send<T: Send>() {}
    assert_send::<Mutation>();
    assert_send::<crate::feed::MutationFeed>();
};

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

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

    fn table() -> Rc<RefCell<SubTable>> {
        Rc::new(RefCell::new(SubTable::default()))
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
        let subs = table();
        let (tx, rx) = bounded(ROOMY);
        let topic = unique_topic();
        let conn = subs.borrow_mut().new_conn();
        subs.borrow_mut().subscribe(tenant, topic, conn, tx);

        // A subscriber on a *different* pivot must not hear this.
        let (other_tx, other_rx) = bounded(ROOMY);
        let other = subs.borrow_mut().new_conn();
        subs.borrow_mut().subscribe(
            tenant,
            Topic {
                struct_hash: 0xABCD,
                pivot: Some(LocalId::new(9, true, 0)),
            },
            other,
            other_tx,
        );

        let store = NotifyStore::new(Rc::new(NullStore), subs);
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
        let subs = table();
        let (tx, rx) = bounded(ROOMY);
        let conn = subs.borrow_mut().new_conn();
        subs.borrow_mut()
            .subscribe(U48::from(3u32), unique_topic(), conn, tx);
        let store = NotifyStore::new(Rc::new(NullStore), subs);
        // Same topic shape, foreign tenant — the key differs.
        store.note_mutation(|| mutation(U48::from(4u32), None));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn unsubscribe_stops_delivery_and_empties_the_table() {
        let tenant = U48::from(5u32);
        let topic = unique_topic();
        let subs = table();
        let (tx, rx) = bounded(ROOMY);
        let conn = subs.borrow_mut().new_conn();
        subs.borrow_mut().subscribe(tenant, topic, conn, tx);
        subs.borrow_mut().unsubscribe(tenant, topic, conn);

        let store = NotifyStore::new(Rc::new(NullStore), Rc::clone(&subs));
        store.note_mutation(|| mutation(tenant, None));
        assert!(rx.try_recv().is_err(), "unsubscribed conn hears nothing");
        assert!(subs.borrow().by_topic.is_empty());
    }

    #[test]
    fn a_closed_receiver_is_pruned_on_publish() {
        let tenant = U48::from(6u32);
        let subs = table();
        let (tx, rx) = bounded(ROOMY);
        let conn = subs.borrow_mut().new_conn();
        subs.borrow_mut()
            .subscribe(tenant, unique_topic(), conn, tx);
        drop(rx); // the session task ended without unregistering.

        let store = NotifyStore::new(Rc::new(NullStore), Rc::clone(&subs));
        store.note_mutation(|| mutation(tenant, None));
        assert!(
            subs.borrow().by_topic.is_empty(),
            "the dead subscription is dropped"
        );
    }

    /// The outbound policy: a client reading slower than its topics change is
    /// disconnected, not buffered without limit. What was queued before the
    /// overflow is still delivered — the client's cursor stops there, and its
    /// catch-up after reconnecting starts from the event that did not fit.
    #[test]
    fn a_full_outbox_disconnects_the_slow_client() {
        let tenant = U48::from(7u32);
        let subs = table();
        let (tx, rx) = bounded(2);
        let conn = subs.borrow_mut().new_conn();
        subs.borrow_mut()
            .subscribe(tenant, unique_topic(), conn, tx);

        let store = NotifyStore::new(Rc::new(NullStore), Rc::clone(&subs));
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
        assert!(subs.borrow().by_topic.is_empty(), "and it is unsubscribed");
    }

    #[test]
    fn disconnect_all_closes_every_outbox() {
        let subs = table();
        let (a_tx, a_rx) = bounded(ROOMY);
        let (b_tx, b_rx) = bounded(ROOMY);
        let a = subs.borrow_mut().new_conn();
        let b = subs.borrow_mut().new_conn();
        subs.borrow_mut()
            .subscribe(U48::from(1u32), unique_topic(), a, a_tx);
        subs.borrow_mut()
            .subscribe(U48::from(2u32), unique_topic(), b, b_tx);

        subs.borrow_mut().disconnect_all();

        assert!(a_rx.is_closed() && b_rx.is_closed());
        assert!(subs.borrow().by_topic.is_empty());
    }
}
