//! Batch barriers on a helper thread (RFC 0067 §4), end to end.
//!
//! 1. a batch acknowledged through the helper is on disk: the store reopens
//!    with it, nothing lost;
//! 2. retirement still completes with barriers on the helper: a checkpoint
//!    followed by writes and another checkpoint deletes the first's retired
//!    journal.

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{Barrier, PageStore, StructStorage};

const SH: u64 = 0x0067_0004_BA22_1E25;
static SLOT: StructStorage = StructStorage::new(SH);

fn open(dir: &std::path::Path) -> PageStore {
    let mut store = PageStore::open(dir, &[&SLOT]).expect("open");
    store.use_barrier(Barrier::spawn("barrier-test").expect("helper"));
    store
}

fn put(k: u64) -> Write {
    let id = Id::new(k, U48::from(1u32), false, 1);
    let mut bytes = SH.to_le_bytes().to_vec();
    bytes.extend_from_slice(&k.to_le_bytes());
    Write::Put(id, bytes)
}

fn journals(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("journal_")
        })
        .count()
}

#[test]
fn an_acknowledged_batch_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = open(dir.path());
        block_on(store.apply(&[put(7)])).expect("durable");
    } // dropped without a checkpoint: only the journal holds the batch
    let store = PageStore::open(dir.path(), &[&SLOT]).expect("reopen");
    let id = Id::new(7, U48::from(1u32), false, 1);
    let got = block_on(store.get_of(SH, id)).unwrap().expect("replayed");
    assert_eq!(&got[8..], &7u64.to_le_bytes());
}

#[test]
fn retirement_sees_the_helpers_barriers() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    block_on(store.apply(&[put(1)])).unwrap();
    store.commit_journal().unwrap();
    assert!(
        store.is_retiring(),
        "the first checkpoint's frame is pending"
    );
    // A batch after the frame: its barrier, taken on the helper, carries the
    // frame too.
    block_on(store.apply(&[put(2)])).unwrap();
    store.commit_journal().unwrap();
    // The first checkpoint's retired journal is gone; what is left is the
    // second's (pending) and the live one.
    assert!(
        journals(dir.path()) <= 2,
        "{} journals",
        journals(dir.path())
    );
}
