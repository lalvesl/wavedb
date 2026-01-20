//! Do the sparse index's element counts still agree with the segments they name
//! **after a crash**? ([RFC 0052]'s last open question.)
//!
//! The agreement itself is pinned in `wavedb-core`, but every test there applies
//! whole batches to an in-memory store that cannot tear. The claim those tests
//! cannot reach is the one the single-batch rule actually makes: a mutation's
//! segment writes and the index writes that describe them land together **or not
//! at all**, so no recovery can produce a chain whose counts have drifted.
//!
//! That matters more than it sounds. A drifted count does not fail — the pager
//! descends by count, so `_at_page` silently returns the wrong rows, and
//! `_len` silently reports a total the list cannot serve. The failure mode is a
//! wrong answer, not an error, which is exactly the kind that needs a test.
//!
//! ## What "crash" means here
//!
//! The store is dropped **without a checkpoint**, so the most recent batches
//! exist only in the journal and recovery has to replay them. Earlier batches
//! are forced out to the data files first, so the walk after reopen crosses both
//! recovery paths — settled pages and replayed frames — inside one chain.
//!
//! ## Why the check runs through the public surface
//!
//! Two public numbers answer it without reaching into the engine, and they
//! disagree exactly when the invariant breaks:
//!
//! - `listed_by_name_len` is the **index**'s answer (the root's subtree sum);
//! - counting what `listed_by_name` yields is the **segments**' answer (the walk
//!   decodes records out of the segments themselves).
//!
//! Comparing them is the user-visible form of "an index entry and its segment
//! disagree", which is a better assertion than poking at internals would be.
//!
//! [RFC 0052]: https://github.com/wavedb/wavedb/blob/main/rfcs/0052-segment-size-as-the-pagination-unit.md

#![allow(clippy::cast_possible_truncation)]
// row counts, bounded by the test
// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use futures::TryStreamExt;
use futures::executor::block_on;
use wavedb_core::{LocalHandle, U48};
use wavedb_macros::wavedb;
use wavedb_storage::PageStore;

/// A record type with a declared list, both chains at a small capacity.
///
/// Small so a few dozen records really do span many segments — and removals
/// really do drive merges — rather than sitting in the single segment a default
/// capacity would give them.
#[wavedb(NonUnique, page = 4)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Entry {
    #[wavedb::list]
    pub name: String,
    pub note: u64,
}

const TENANT: u32 = 42;

fn open(path: &std::path::Path) -> PageStore {
    PageStore::open(path, &Entry::storage_entries()).unwrap()
}

fn entry(n: u32) -> Entry {
    Entry {
        name: format!("e{n:03}"),
        note: 0,
    }
}

/// The two answers, and the rows themselves.
async fn observe(
    db: &LocalHandle<'_, PageStore>,
    col: wavedb_core::CollectionHandle<Entry>,
) -> (u64, Vec<String>) {
    use wavedb_core::CollectionHandle;
    let total = CollectionHandle::list_len(&col, db, 0).await.unwrap();
    let rows: Vec<Entry> = CollectionHandle::listed(&col, db, 0)
        .try_collect()
        .await
        .unwrap();
    (total, rows.into_iter().map(|e| e.name).collect())
}

#[test]
fn a_crash_cannot_leave_the_counts_disagreeing_with_the_segments() {
    let dir = tempfile::tempdir().unwrap();
    let tenant = U48::from(TENANT);

    let (pivot, before) = block_on(async {
        let store = open(dir.path());
        let db = LocalHandle::new(&store, tenant);
        let pivot = Entry::create_pivot(&db).await.unwrap();
        let col = Entry::collection(pivot);

        // Phase 1 — grow past several splits, then force everything out to
        // the data files. These records recover from settled pages.
        let mut ids = Vec::new();
        for n in 0..24u32 {
            ids.push(col.insert(&db, &entry(n)).await.unwrap());
        }
        store.drain().unwrap();

        // Phase 2 — more traffic, deliberately *not* drained: saves that
        // relocate a record inside both chains, removals that drive merges, and
        // fresh inserts. These recover by journal replay.
        for (i, id) in ids.iter().enumerate().take(6) {
            let renamed = Entry {
                // Sorts below everything, so the list relocates the record to
                // its head — the index must revise two counts, not one.
                name: format!("a{i:03}"),
                note: 7,
            };
            col.save(&db, *id, &renamed).await.unwrap();
        }
        for id in ids.iter().skip(12).take(8) {
            assert!(col.remove(&db, *id).await.unwrap());
        }
        for n in 100..108u32 {
            col.insert(&db, &entry(n)).await.unwrap();
        }

        let before = observe(&db, col).await;
        assert_eq!(
            before.0,
            before.1.len() as u64,
            "counts already disagreed before any crash"
        );
        assert_eq!(before.0, 24, "24 inserted − 8 removed + 8 inserted");
        (pivot, before)
        // `store` drops here with the phase-2 batches still journal-only:
        // the process "died" before a checkpoint.
    });

    // Recovery, and the same two questions.
    block_on(async {
        let store = open(dir.path());
        let db = LocalHandle::new(&store, tenant);
        let col = Entry::collection(pivot);
        let after = observe(&db, col).await;

        assert_eq!(
            after.0,
            after.1.len() as u64,
            "after recovery the index reports {} rows but the segments hold \
             {} — a pager would serve the difference silently",
            after.0,
            after.1.len()
        );
        assert_eq!(after, before, "recovery changed the list");

        // The rows are in the declared order, and the relocated ones really did
        // move to the head — a count that agreed by coincidence (both halves
        // losing the same record) would not survive this.
        let mut sorted = after.1.clone();
        sorted.sort();
        assert_eq!(after.1, sorted, "the list came back out of order");
        assert!(
            after.1.iter().take(6).all(|n| n.starts_with('a')),
            "the saves that relocated records did not survive recovery: {:?}",
            &after.1[..6]
        );

        // And the descent agrees with the walk: entering at an offset must land
        // on the row the ordered walk has there. This is the count's *only*
        // consumer, so a drift that both other assertions missed shows up here.
        let page: Vec<Entry> =
            wavedb_core::CollectionHandle::listed_at(&col, &db, 0, 20)
                .try_collect()
                .await
                .unwrap();
        assert_eq!(
            page.first().map(|e| e.name.clone()),
            after.1.get(20).cloned(),
            "the offset descent and the ordered walk disagree at 20"
        );
    });
}
