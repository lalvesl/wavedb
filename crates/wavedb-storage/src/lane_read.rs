//! Reading an ordered lane ([RFC 0078] §7) — the entries of a span, in
//! either order, from the lane's pages with the record cache's keys merged
//! over them.
//!
//! The pages are walked from the span's edge, one entry's coverage at a
//! time. Coverages are contiguous — an entry covers up to where the next one
//! starts — so the keys between two pages' records are only ever in the
//! cache, which the cache's ordered keys of the lane answer. A walk stops
//! once it holds what the span skips and returns, so a read near an edge
//! reads the pages there and no others.
//!
//! An evicted page is brought back first, as a write would: the walk stops
//! at it, the page is restored, and the walk starts over.
//!
//! In a counted lane (a declared list's) a walk passes a page without
//! reading it when the span covers it whole, its count is known and the
//! cache holds nothing of its range unsettled: the count is then exactly
//! what reading it would yield. So a skip into a list, or its length, sums
//! counts in RAM and reads only the pages at its edges.
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use wavedb_core::store::{Entry, Span};

use crate::error::StorageResult;
use crate::lane_walk::{Walk, below};
use crate::page_store::PageStore;

/// What one synchronous walk came to.
pub enum Walked {
    /// The span's entries past those it skipped, and how many it skipped.
    Done { entries: Vec<Entry>, skipped: u64 },
    /// It met the evicted page of slot `idx` that `key` finds.
    Evicted { idx: usize, key: Vec<u8> },
}

impl PageStore {
    /// The synchronous half of a scan of the ordered lane `lane`: the
    /// record cache and the pages on disk — all of it, on a disk-only
    /// store, so its scan is ready when it is called.
    ///
    /// # Errors
    /// A page read or decode fault.
    pub(crate) fn scan_now(
        &self,
        lane: u64,
        span: &Span,
    ) -> StorageResult<Walked> {
        self.walk(lane, span, span.skip, span.limit)
    }

    /// The synchronous half of a count of `[lo, hi)` in the ordered lane
    /// `lane`: a walk skipping every entry, its count what it skipped.
    ///
    /// # Errors
    /// A page read or decode fault.
    pub(crate) fn count_now(
        &self,
        lane: u64,
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> StorageResult<Walked> {
        let span = Span {
            lo: lo.to_vec(),
            hi: hi.map(<[u8]>::to_vec),
            rev: false,
            skip: u64::MAX,
            limit: usize::MAX,
        };
        self.walk(lane, &span, u64::MAX, usize::MAX)
    }

    /// Finish a walk [`scan_now`](Self::scan_now) or
    /// [`count_now`](Self::count_now) began: each evicted page it meets
    /// restored, and the walk made `again`. Its entries, and how many it
    /// skipped.
    ///
    /// # Errors
    /// As [`scan_now`](Self::scan_now); an object-store fault restoring a
    /// page.
    pub(crate) async fn walk_rest(
        &self,
        mut now: StorageResult<Walked>,
        again: impl Fn() -> StorageResult<Walked>,
    ) -> StorageResult<(Vec<Entry>, u64)> {
        loop {
            match now? {
                Walked::Done { entries, skipped } => {
                    return Ok((entries, skipped));
                }
                Walked::Evicted { idx, key } => {
                    self.restore(idx, &key).await?;
                }
            }
            now = again();
        }
    }

    /// Walk lane `lane`'s pages along `span`, past `skip` entries, until
    /// `need` entries are held.
    fn walk(
        &self,
        lane: u64,
        span: &Span,
        skip: u64,
        need: usize,
    ) -> StorageResult<Walked> {
        let done = Walked::Done {
            entries: Vec::new(),
            skipped: 0,
        };
        let Some(idx) = self.slot_index(lane) else {
            return Ok(done);
        };
        if !below(&span.lo, span.hi.as_deref()) {
            return Ok(done);
        }
        let index = self.types[idx].directory().borrow();
        let mut walk = Walk {
            store: self,
            idx,
            index: index.as_ref(),
            span,
            skip,
            skipped: 0,
            out: Vec::new(),
        };
        let evicted = if span.rev {
            walk.back(need)?
        } else {
            walk.forth(need)?
        };
        walk.out.truncate(need);
        Ok(match evicted {
            Some(key) => Walked::Evicted { idx, key },
            None => Walked::Done {
                entries: walk.out,
                skipped: walk.skipped,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use wavedb_core::store::Span;
    use wavedb_core::{Store, Write};

    use crate::page_store::PageStore;
    use crate::struct_storage::StructStorage;

    const PLAIN: u64 = 0x0078_0007_1A4E_0001;
    static PLAIN_SLOT: StructStorage = StructStorage::new(PLAIN);
    const COUNTED: u64 = 0x0078_0007_1A4E_0002;
    static COUNTED_SLOT: StructStorage = StructStorage::new(COUNTED).counted();

    const ROWS: u64 = 2_000;

    fn key(collection: u8, at: u64) -> Vec<u8> {
        let mut key = vec![collection; 16];
        key.extend_from_slice(&at.to_be_bytes());
        key
    }

    /// 200 bytes that do not compress away, so pages fill.
    fn value(at: u64) -> Vec<u8> {
        let mut x = at.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..200)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x.to_le_bytes()[0]
            })
            .collect()
    }

    /// `ROWS` entries of collection 1 between a few of its neighbours',
    /// settled.
    fn filled(store: &PageStore, lane: u64) {
        let mut writes = Vec::new();
        for at in 0..ROWS {
            writes.push(Write::Set(lane, key(1, at), value(at)));
        }
        for at in 0..10 {
            writes.push(Write::Set(lane, key(0, at), value(at)));
            writes.push(Write::Set(lane, key(2, at), value(at)));
        }
        block_on(store.apply(&writes)).unwrap();
        block_on(store.apply_durable(&[])).unwrap();
    }

    /// Pages a closure read: what the emptied page cache holds after it.
    fn pages_read(store: &PageStore, read: impl FnOnce()) -> usize {
        store.file.cache.clear();
        read();
        store.file.cache.len()
    }

    /// A count of a collection, and a skip deep into it, read its edge
    /// pages in a counted lane and every page in a plain one — the same
    /// answers either way.
    #[test]
    fn a_counted_lane_counts_and_skips_reading_only_its_edges() {
        let collection = Span::prefix(&[1; 16], 5);
        let deep = Span {
            skip: 1_500,
            ..collection.clone()
        };
        for lane in [PLAIN, COUNTED] {
            let dir = tempfile::tempdir().unwrap();
            let store =
                PageStore::open(dir.path(), &[&PLAIN_SLOT, &COUNTED_SLOT])
                    .unwrap();
            filled(&store, lane);
            let pages = store.bucket_count(lane);
            assert!(pages > 10, "{pages} pages");
            let (lo, hi) = (collection.lo.clone(), collection.hi.clone());

            let mut n = 0;
            let counting = pages_read(&store, || {
                n = block_on(store.count(lane, &lo, hi.as_deref())).unwrap();
            });
            assert_eq!(n, ROWS);
            let mut got = Vec::new();
            let skipping = pages_read(&store, || {
                got = block_on(store.scan(lane, &deep)).unwrap();
            });
            let at: Vec<_> = (1_500..1_505).map(|at| key(1, at)).collect();
            assert_eq!(got.into_iter().map(|(k, _)| k).collect::<Vec<_>>(), at);

            if lane == COUNTED {
                assert!(counting <= 2, "a count read {counting} pages");
                assert!(skipping <= 3, "a skip read {skipping} pages");
            } else {
                assert!(counting + 2 >= pages, "a count read {counting}");
                assert!(skipping * 2 >= pages, "a skip read {skipping}");
            }

            // A write not settled yet over the pages: they are read again
            // where it lands, and the answers follow it.
            block_on(store.apply(&[Write::Unset(lane, key(1, 10))])).unwrap();
            let n = block_on(store.count(lane, &lo, hi.as_deref())).unwrap();
            assert_eq!(n, ROWS - 1);
            let got = block_on(store.scan(lane, &deep)).unwrap();
            assert_eq!(got[0].0, key(1, 1_501));
        }
    }

    /// A page holding more than one collection is cut at their edge. Two
    /// lists of one owner differ in their declaration byte alone, so a
    /// range refines to that depth before it parts them, and there the
    /// large one spreads over many quanta: a cut at the middle would leave
    /// the small one sharing its page with the large one's head.
    #[test]
    fn a_page_is_cut_at_the_edge_between_collections() {
        let list = |declaration: u8, at: u64| {
            let mut key = vec![7; 16];
            key.push(declaration);
            key.extend_from_slice(&(at << 52).to_be_bytes());
            key
        };
        let dir = tempfile::tempdir().unwrap();
        let store =
            PageStore::open(dir.path(), &[&PLAIN_SLOT, &COUNTED_SLOT]).unwrap();
        let mut writes = Vec::new();
        for at in 0..10 {
            writes.push(Write::Set(COUNTED, list(0, at), value(at)));
        }
        for at in 0..ROWS {
            writes.push(Write::Set(COUNTED, list(1, at), value(at)));
        }
        block_on(store.apply(&writes)).unwrap();
        block_on(store.apply_durable(&[])).unwrap();
        let types = &store.types;
        let slot = types.iter().position(|t| t.struct_hash() == COUNTED);
        let index = types[slot.unwrap()].directory().borrow();
        let index = index.as_ref().unwrap();
        let crate::ordered::Found::Page { at, .. } = index.find(&list(0, 0))
        else {
            panic!("the small list has a page");
        };
        assert_eq!(index.count_at(at), Some(10));
    }
}
