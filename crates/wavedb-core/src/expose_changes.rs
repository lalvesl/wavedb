//! The `Changes` command's engine — catch-up **by navigation** (W6).
//!
//! A reconnecting client holds a cursor: the greatest node instant it has
//! seen for a topic. Answering "what changed since?" never replays a log
//! of requests — the disk structures phase 3 laid down *are* the answer:
//!
//! - a **collection** reads its recency chain and its removal log past the
//!   cursor (RFC 0050) — every record inserted or updated since (once, at
//!   its live instant) and every removal (at its removal instant). Both are
//!   ordered by instant, so each read starts just past the cursor's key and
//!   runs to the chain's end; a caught-up client pays two empty reads for
//!   "nothing new";
//! - a **Unique** walks its chain forward from the client's own version:
//!   the derived slot of the cursor instant holds its archive, whose
//!   `Next` names the successor — a MISS means the successor is the live
//!   anchor. Every missed version ships with its metadata rebuilt to the
//!   live form (`CreatedAt(its instant)`), so a mirror adopting them in
//!   order replays the chain byte-identically.
//!
//! `since: None` is **registration**: answer the topic's current tail as
//! the starting cursor, with no events — a fresh watch begins at "now",
//! never with a surprise full-set replay.

use futures::TryStreamExt;

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::expose::Reply;
use crate::id::Id;
use crate::index::{Chain, SecKey};
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::record;
use crate::store::Store;
use crate::traits::{NonUniqueStruct, WaveDbStruct};
use crate::u48::U48;
use crate::wire::{WaveWire, to_wire};

/// One catch-up item — what each `Reply::Values` entry of a `Changes`
/// answer encodes.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum Change {
    /// The topic's cursor after this answer — always the first entry.
    /// Every later `Changes` call passes it back as `since`.
    Cursor(u64),
    /// A version the caller has not seen: its authoritative id, its
    /// [`Metadata`] in live form (adopt it verbatim), and its body bytes.
    Saved(Id, Metadata, Vec<u8>),
    /// A removal since the cursor: the record's anchor id and the removal
    /// instant its dead-log entry is keyed by.
    Removed(Id, u64),
}

/// One chain's tail past `cursor`: every entry whose key instant is
/// strictly greater, as `(instant, anchor, payload)`, ascending — one read
/// from the least key past the cursor to the chain's end.
async fn tail_since<P, S>(
    chain: &Chain<P>,
    store: &S,
    cursor: u64,
) -> Result<Vec<(u64, LocalId, P)>>
where
    P: WaveWire + Clone,
    S: Store,
{
    let Some(next) = cursor.checked_add(1) else {
        return Ok(Vec::new());
    };
    let from = SecKey {
        field: next.to_be_bytes().to_vec(),
        rec: LocalId::default(),
    };
    chain
        .scan_from(store, &from)
        .map_ok(|(key, load)| (key_instant(&key), key.rec, load))
        .try_collect()
        .await
}

/// The instant a chain key encodes (their field IS the 8-byte big-endian
/// instant).
fn key_instant(key: &SecKey) -> u64 {
    key.field
        .as_slice()
        .try_into()
        .map_or(0, u64::from_be_bytes)
}

/// A collection's changes since `cursor`, `Cursor` first.
///
/// The record chain's tail (the live version of every record inserted or
/// updated since) merged with the removal log's (removals), in instant
/// order. `None` = register at the collection's current tail with no
/// events.
///
/// The chains name the records; each `Saved` reads its record at its
/// anchor.
///
/// # Errors
/// Propagates a [`Store`] failure, [`Error::PivotMissing`] on a stale
/// pivot, or a decode fault on a corrupt record.
///
/// [`Error::PivotMissing`]: crate::Error::PivotMissing
pub async fn collection_changes<T, S>(
    store: &S,
    pivot: LocalId,
    tenant: U48,
    since: Option<u64>,
) -> Result<Reply>
where
    T: NonUniqueStruct,
    S: Store,
{
    let col = Collection::<T>::at(pivot, tenant);
    col.check_pivot(store).await?;
    let Some(cursor) = since else {
        let tail = col.instant_floor(store).await?;
        return Ok(values(tail, &[]));
    };

    let saved = tail_since(&col.recency_chain(), store, cursor).await?;
    let removed = tail_since(&col.dead_log(), store, cursor).await?;

    // Merge the two tails into one instant-ordered timeline.
    let mut timeline = Vec::with_capacity(saved.len() + removed.len());
    for (instant, rec, ()) in saved {
        let id = rec.to_id(tenant);
        // The chain names the record; the record is at its anchor.
        let bytes = store
            .get_of(T::STRUCT_HASH, id)
            .await?
            .ok_or(crate::Error::RecordMissing(id))?;
        let (meta, body) = record::split_record(T::STRUCT_HASH, &bytes)?;
        timeline.push((instant, Change::Saved(id, meta, body.to_vec())));
    }
    timeline.extend(removed.into_iter().map(|(instant, rec, ())| {
        (instant, Change::Removed(rec.to_id(tenant), instant))
    }));
    timeline.sort_unstable_by_key(|(instant, _)| *instant);

    // Sorted, so the last entry carries the tail the caller comes back with.
    let new_cursor = timeline
        .last()
        .map_or(cursor, |(instant, _)| *instant)
        .max(cursor);
    let changes: Vec<Change> =
        timeline.into_iter().map(|(_, change)| change).collect();
    Ok(values(new_cursor, &changes))
}

/// A `Unique`'s changes since `cursor`: the chain walked **forward**.
///
/// Starting at the client's own version, each missed version arrives in
/// authoring order, the live one last. An unknown cursor (its derived
/// slot misses while the live record is newer) degrades to the live
/// version alone. `None` = register at the live version's instant (or
/// `0` when never saved) with no events.
///
/// # Errors
/// Propagates a [`Store`] failure, a decode fault, or
/// [`Error::ChainCorrupt`] on a forward link that does not advance.
pub async fn unique_changes<T, S>(
    store: &S,
    tenant: U48,
    since: Option<u64>,
) -> Result<Reply>
where
    T: WaveDbStruct,
    S: Store,
{
    let anchor = Id::new(T::STRUCT_HASH, tenant, true, 0);
    let Some(live_bytes) = store.get_of(T::STRUCT_HASH, anchor).await? else {
        return Ok(values(since.unwrap_or(0), &[]));
    };
    let (live_meta, live_body) =
        record::split_record(T::STRUCT_HASH, &live_bytes)?;
    let Succession::CreatedAt(live_instant) = live_meta.succession else {
        return Err(Error::ChainCorrupt(anchor));
    };
    let Some(cursor) = since else {
        return Ok(values(live_instant, &[]));
    };
    if live_instant <= cursor {
        return Ok(values(cursor, &[]));
    }

    let mut changes = Vec::new();
    let mut at = cursor;
    while at > 0 && at < live_instant {
        let slot = record::archive_id(T::STRUCT_HASH, T::SHAPE, at, tenant);
        let Some(bytes) = store.get_of(T::STRUCT_HASH, slot).await? else {
            // The version at `at` was never archived here — an unknown
            // cursor (or a gap): what survives is the live state below.
            break;
        };
        let (meta, body) = record::split_record(T::STRUCT_HASH, &bytes)?;
        let Succession::Next(next) = meta.succession else {
            return Err(Error::ChainCorrupt(slot));
        };
        if next <= at {
            return Err(Error::ChainCorrupt(slot));
        }
        if at > cursor {
            // A version the client missed — shipped as it looked when
            // live, so adopting it replays the chain exactly.
            let live_form = Metadata {
                succession: Succession::CreatedAt(at),
                ..meta
            };
            changes.push(Change::Saved(anchor, live_form, body.to_vec()));
        }
        at = next;
    }
    changes.push(Change::Saved(anchor, live_meta, live_body.to_vec()));
    Ok(values(live_instant, &changes))
}

/// Assemble the `Changes` reply: the cursor entry first, then each change,
/// every entry wire-encoded.
fn values(cursor: u64, changes: &[Change]) -> Reply {
    let mut entries = Vec::with_capacity(1 + changes.len());
    entries.push(to_wire(&Change::Cursor(cursor)));
    entries.extend(changes.iter().map(to_wire));
    Reply::Values(entries)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use futures::executor::block_on;

    use super::{Change, collection_changes, unique_changes};
    use crate::collection::Collection;
    use crate::error::Result;
    use crate::expose::Reply;
    use crate::id::Id;
    use crate::index::Pivot;
    use crate::index::mem_store::MemStore;
    use crate::metadata::Succession;
    use crate::permission::PermissionRef;
    use crate::record::{adopt_unique, get_unique, save_unique};
    use crate::store::Store;
    use crate::traits::{NonUniqueStruct, Shape, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::{WaveWire, from_wire};

    const TENANT: u32 = 11;

    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Doc {
        n: u64,
    }
    impl WaveDbStruct for Doc {
        const STRUCT_HASH: u64 = 0xC4A6_0001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }
    impl NonUniqueStruct for Doc {
        type Pivot = DocPivot;
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct DocPivot {
        permission: Option<PermissionRef>,
    }
    impl Pivot for DocPivot {
        const STRUCT_HASH: u64 = 0xC4A6_0002;
        fn permission(&self) -> Option<&PermissionRef> {
            self.permission.as_ref()
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Cfg {
        v: u64,
    }
    impl WaveDbStruct for Cfg {
        const STRUCT_HASH: u64 = 0xC4A6_0003;
        const SHAPE: Shape = Shape::Unique;
        type PivotId = ();
    }

    fn tenant() -> U48 {
        U48::from(TENANT)
    }

    /// A [`Store`] that records every address it is asked to read. Phase
    /// 5b's claim is about *which* addresses a catch-up touches, so the
    /// test has to see them.
    struct Watched<'a> {
        inner: &'a MemStore,
        reads: Mutex<Vec<Id>>,
        scans: Mutex<usize>,
    }

    impl<'a> Watched<'a> {
        fn new(inner: &'a MemStore) -> Self {
            Self {
                inner,
                reads: Mutex::new(Vec::new()),
                scans: Mutex::new(0),
            }
        }

        fn reads(&self) -> Vec<Id> {
            self.reads.lock().unwrap().clone()
        }

        fn scans(&self) -> usize {
            *self.scans.lock().unwrap()
        }
    }

    impl Store for Watched<'_> {
        async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
            self.reads.lock().unwrap().push(id);
            self.inner.get(id).await
        }

        async fn scan(
            &self,
            lane: u64,
            span: &crate::store::Span,
        ) -> Result<Vec<crate::store::Entry>> {
            *self.scans.lock().unwrap() += 1;
            self.inner.scan(lane, span).await
        }

        async fn count(
            &self,
            lane: u64,
            lo: &[u8],
            hi: Option<&[u8]>,
        ) -> Result<u64> {
            self.inner.count(lane, lo, hi).await
        }

        async fn apply(&self, batch: &[crate::store::Write]) -> Result<()> {
            self.inner.apply(batch).await
        }
    }

    fn decode(reply: Reply) -> (u64, Vec<Change>) {
        let Reply::Values(entries) = reply else {
            panic!("changes answer as values");
        };
        let mut changes: Vec<Change> = entries
            .iter()
            .map(|bytes| from_wire::<Change>(bytes).unwrap())
            .collect();
        let Change::Cursor(cursor) = changes.remove(0) else {
            panic!("the cursor rides first");
        };
        (cursor, changes)
    }

    #[test]
    fn collection_changes_walk_the_log_tails_from_the_cursor() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());

            // Registration on an empty collection: tail 0, no events.
            let (start, changes) = decode(
                collection_changes::<Doc, _>(&store, pivot, tenant(), None)
                    .await
                    .unwrap(),
            );
            assert_eq!((start, changes.len()), (0, 0));

            let a = col.insert(&store, &Doc { n: 1 }).await.unwrap();
            let b = col.insert(&store, &Doc { n: 2 }).await.unwrap();
            col.save(&store, a, &Doc { n: 10 }).await.unwrap();
            assert!(col.remove(&store, b).await.unwrap());

            // Since the registration cursor: `a` once (its live version,
            // metadata riding for verbatim adoption), `b`'s removal —
            // instant order, the removal last.
            let (cursor, changes) = decode(
                collection_changes::<Doc, _>(
                    &store,
                    pivot,
                    tenant(),
                    Some(start),
                )
                .await
                .unwrap(),
            );
            assert_eq!(changes.len(), 2);
            let Change::Saved(id, meta, body) = &changes[0] else {
                panic!("the live record first (older instant)");
            };
            assert_eq!(*id, a);
            assert_eq!(from_wire::<Doc>(body).unwrap(), Doc { n: 10 });
            let Succession::CreatedAt(a_live) = meta.succession else {
                panic!("a live version's metadata");
            };
            let Change::Removed(gone, removed_at) = &changes[1] else {
                panic!("the removal last (newest instant)");
            };
            assert_eq!(*gone, b);
            assert!(*removed_at > a_live);
            assert_eq!(cursor, *removed_at, "the cursor advances to the tail");

            // Caught up: nothing past the new cursor.
            let (again, rest) = decode(
                collection_changes::<Doc, _>(
                    &store,
                    pivot,
                    tenant(),
                    Some(cursor),
                )
                .await
                .unwrap(),
            );
            assert_eq!((again, rest.len()), (cursor, 0));
        });
    }

    #[test]
    fn a_collection_catch_up_reads_what_changed_not_the_collection() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());

            let n = 40u64;
            let mut anchors = Vec::new();
            for i in 0..n {
                anchors.push(col.insert(&store, &Doc { n: i }).await.unwrap());
            }

            // Everything since the beginning of time.
            let watched = Watched::new(&store);
            let (cursor, changes) = decode(
                collection_changes::<Doc, _>(
                    &watched,
                    pivot,
                    tenant(),
                    Some(0),
                )
                .await
                .unwrap(),
            );
            let bodies: Vec<u64> = changes
                .iter()
                .map(|c| {
                    let Change::Saved(_, _, body) = c else {
                        panic!("inserts only")
                    };
                    from_wire::<Doc>(body).unwrap().n
                })
                .collect();
            assert_eq!(bodies, (0..n).collect::<Vec<_>>());

            // The claim, restated for RFC 0054's no-duplication default: the
            // *finding* is a read of each chain even though the *bodies* are
            // not. The chain holds pointers, so a change's record is fetched
            // at its anchor — but only for records that actually changed, and
            // finding which ones took one read of each chain rather than a
            // walk over the collection.
            let reads = watched.reads();
            assert!(
                anchors.iter().all(|id| reads.contains(id)),
                "every changed record is resolved at its anchor"
            );
            // 40 changes: 40 anchors + the pivot, and one read per chain.
            assert_eq!(reads.len(), anchors.len() + 1);
            assert_eq!(watched.scans(), 2, "one read of each chain");

            // And a caught-up client pays a constant: the pivot, and one
            // empty read of each chain.
            let watched = Watched::new(&store);
            let (again, rest) = decode(
                collection_changes::<Doc, _>(
                    &watched,
                    pivot,
                    tenant(),
                    Some(cursor),
                )
                .await
                .unwrap(),
            );
            assert_eq!((again, rest.len()), (cursor, 0));
            assert_eq!(watched.reads().len(), 1, "the pivot");
            assert_eq!(watched.scans(), 2, "reads for 'nothing new'");
        });
    }

    #[test]
    fn unique_changes_replay_the_chain_and_mirrors_converge_byte_identical() {
        block_on(async {
            let node = MemStore::default();
            save_unique(&node, tenant(), &Cfg { v: 1 }).await.unwrap();

            // A mirror adopts V1 while it is live (cursor = its instant).
            let (v1_cursor, changes) = decode(
                unique_changes::<Cfg, _>(&node, tenant(), Some(0))
                    .await
                    .unwrap(),
            );
            let cache = MemStore::default();
            for change in changes {
                let Change::Saved(_, meta, body) = change else {
                    panic!("a unique never removes");
                };
                adopt_unique(
                    &cache,
                    tenant(),
                    meta,
                    &from_wire::<Cfg>(&body).unwrap(),
                )
                .await
                .unwrap();
            }

            // The node moves on twice while the mirror is away.
            save_unique(&node, tenant(), &Cfg { v: 2 }).await.unwrap();
            save_unique(&node, tenant(), &Cfg { v: 3 }).await.unwrap();

            // Catch-up from the mirror's cursor: V2 then V3, oldest first,
            // each in live form — adopting in order replays the chain.
            let (cursor, changes) = decode(
                unique_changes::<Cfg, _>(&node, tenant(), Some(v1_cursor))
                    .await
                    .unwrap(),
            );
            let versions: Vec<u64> = changes
                .iter()
                .map(|c| {
                    let Change::Saved(_, _, body) = c else {
                        panic!("saves only")
                    };
                    from_wire::<Cfg>(body).unwrap().v
                })
                .collect();
            assert_eq!(versions, vec![2, 3]);
            for change in changes {
                let Change::Saved(_, meta, body) = change else {
                    unreachable!()
                };
                adopt_unique(
                    &cache,
                    tenant(),
                    meta,
                    &from_wire::<Cfg>(&body).unwrap(),
                )
                .await
                .unwrap();
            }
            assert_eq!(
                get_unique::<Cfg, _>(&cache, tenant())
                    .await
                    .unwrap()
                    .map(|w| w.decode().unwrap()),
                Some(Cfg { v: 3 })
            );
            // The strongest claim: every stored byte — the live anchor AND
            // both archives at their derived slots — matches the node.
            for (id, node_bytes) in node.entries() {
                assert_eq!(
                    Store::get(&cache, id).await.unwrap().as_deref(),
                    Some(node_bytes.as_slice()),
                    "the mirror must hold the node's exact bytes at {id:?}"
                );
            }

            // Caught up; and a cursor the chain never knew degrades to the
            // live version alone.
            let (again, rest) = decode(
                unique_changes::<Cfg, _>(&node, tenant(), Some(cursor))
                    .await
                    .unwrap(),
            );
            assert_eq!((again, rest.len()), (cursor, 0));
            let (_, unknown) = decode(
                unique_changes::<Cfg, _>(&node, tenant(), Some(v1_cursor + 1))
                    .await
                    .unwrap(),
            );
            assert_eq!(unknown.len(), 1, "unknown cursor → the live version");
        });
    }
}
