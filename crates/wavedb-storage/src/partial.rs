//! Rewriting a page in part ([RFC 0081] §2–§3) — split from
//! [`crate::shape`], whose rounds route the records.
//!
//! A round that changes a page in one span of its keys — an update in its
//! middle, a removal, appends past its last key — writes that **span**
//! alone, under an entry of its own, and leaves the page as it is: the
//! entry before the span still names it, and so does a new one after the
//! span, which takes the page's keys from there on. The page written once
//! is not written again for a change of a few hundred bytes, which is what
//! a durable round paid for most: a page of tens of KiB, rewritten whole.
//!
//! This is the RFC's slice, as entries: a slice over a range costs about
//! what the two entries a span adds do, and every reader of the index
//! already knows entries — a lookup is routed by them, and whatever walks a
//! page's records takes it through its entry (`pages::clip`), so the keys
//! the page still holds past its entries are read from the span's run. A
//! page named twice is counted (`crate::shares`).
//!
//! **Fragments** are the span past a page's last key: the appends of a
//! lane keyed by time, or of a collection's tail. When the span's first key
//! shares a quantum with the page's key before it, the span takes that
//! quantum — the cut falls on a quantum boundary — and so on the other side.
//! A span emptied by removals is a hole: the keys the page holds there are
//! gone.
//!
//! A page of one block is rewritten whole: the span would cost the same
//! block and entries more. So is one whose span would not take fewer
//! blocks than it.
//!
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use std::cmp::{max, min};

use crate::error::{StorageError, StorageResult};
use crate::ordered::{Cut, CutOutcome, Found, Placed, Slot};
use crate::page::SlotPage;
use crate::pages::{PENDING, Pages, run_of};
use crate::shape::{Rules, Shaped, Staged, cut_down};

/// Write the span of `st` the round changed under an entry of its own, if
/// that is worth it (see the module docs): `None` then, and `None` for a
/// page the round left as it was. Otherwise `st`, to be rewritten whole.
///
/// # Errors
/// An entry lost; the encoder's faults.
pub fn partial<E>(
    index: &mut Pages,
    st: Staged,
    rules: &Rules,
    encode: &mut E,
    shaped: &mut Shaped,
) -> StorageResult<Option<Staged>>
where
    E: FnMut(&mut SlotPage) -> StorageResult<Vec<u8>>,
{
    let Some(old) = st.old else {
        return Ok(Some(st));
    };
    // A removal of a key the page does not hold changed nothing.
    let (Some(lo), Some(hi)) = (st.changed.first(), st.changed.last()) else {
        return Ok(None);
    };
    if !rules.partial || old.blocks < 2 {
        return Ok(Some(st));
    }
    let keys: Vec<Vec<u8>> = st.page.iter().map(|(k, _)| k.to_vec()).collect();
    let i0 = keys.partition_point(|k| k < lo);
    let j0 = keys.partition_point(|k| k <= hi);
    if keys.is_empty() || !worth(&st, (i0, j0), old.blocks) {
        return Ok(Some(st));
    }
    let bounds = Bounds {
        keys: &keys,
        lo,
        hi,
    };
    let Some((i0, j0)) = bounds.cut(index, (i0, j0), old)? else {
        return Ok(Some(st));
    };
    let n = keys.len();
    // The page's keys either side of the span, under the entries naming it.
    if i0 > 0 {
        let at = entry(index, &keys[0])?;
        index.set_count(at, i0);
    }
    if j0 < n {
        let at = entry(index, &keys[j0])?;
        index.set_count(at, n - j0);
    }
    if i0 > 0 && j0 < n {
        shaped.named.push(run_of(old));
    }
    let start = if i0 > 0 {
        bounds.first_of_span(i0)
    } else {
        bounds.first()
    };
    let mut span = SlotPage::new(st.page.struct_hash());
    for (key, bytes) in st.page.iter().skip(i0).take(j0 - i0) {
        span.upsert(key, bytes.to_vec());
    }
    if span.is_empty() {
        // Never dropped: its quanta would go to an entry naming the page,
        // which still holds what the round removed.
        let at = entry(index, start)?;
        let flags = if rules.keep_holes { PENDING } else { 0 };
        index.punch(at, flags);
        shaped.punched += u64::from(rules.keep_holes);
    } else {
        cut_down(index, span, rules, encode, shaped)?;
    }
    Ok(None)
}

/// Whether writing the keys `[i0, j0)` of `st`'s page alone is worth it,
/// by their bytes: it takes fewer blocks than the page of `blocks` blocks
/// written whole, and the page keeps at least half of what it held live —
/// a span leaves the page holding its old records there, which the cleaner
/// takes back, so a round that empties most of a page rewrites it.
fn worth(st: &Staged, (i0, j0): (usize, usize), blocks: u64) -> bool {
    let total = bytes(&st.page, 0, usize::MAX);
    let span = bytes(&st.page, i0, j0.saturating_sub(i0));
    let kept = total - span;
    span * blocks <= total * (blocks - 1) && kept * 2 >= st.held
}

/// The bytes of `count` records of `page` from the `skip`-th.
pub fn bytes(page: &SlotPage, skip: usize, count: usize) -> u64 {
    page.iter()
        .skip(skip)
        .take(count)
        .map(|(k, v)| (k.len() + v.len()) as u64)
        .sum()
}

/// The keys of a page after the round — and the first and last the round
/// changed, which a removal took out of them — around which its span is
/// cut.
struct Bounds<'k> {
    keys: &'k [Vec<u8>],
    lo: &'k [u8],
    hi: &'k [u8],
}

impl Bounds<'_> {
    /// The page's first key, or the first the round changed before it.
    fn first(&self) -> &[u8] {
        min(&self.keys[0][..], self.lo)
    }

    fn last(&self) -> &[u8] {
        max(&self.keys[self.keys.len() - 1][..], self.hi)
    }

    /// Where a span starting at key `i0` starts.
    fn first_of_span(&self, i0: usize) -> &[u8] {
        self.keys.get(i0).map_or(self.lo, |k| min(&k[..], self.lo))
    }

    /// Where a span ending before key `j0` ends.
    fn last_of_span(&self, j0: usize) -> &[u8] {
        j0.checked_sub(1)
            .map_or(self.hi, |j| max(&self.keys[j][..], self.hi))
    }

    /// Cut the page's entry around the keys `[i0, j0)` — the page named
    /// after them, a stand-in for the span — widening the span to a
    /// quantum boundary where a cut cannot part two keys. The span, or
    /// `None` when it widened to the whole page and nothing was cut.
    fn cut(
        &self,
        index: &mut Pages,
        (mut i0, mut j0): (usize, usize),
        page: Placed<u8>,
    ) -> StorageResult<Option<(usize, usize)>> {
        let n = self.keys.len();
        while j0 < n {
            let at = entry(index, self.first())?;
            let cut = Cut {
                first: self.first(),
                left_last: self.last_of_span(j0),
                right_first: &self.keys[j0],
                last: self.last(),
            };
            match index.cut(at, &cut, page) {
                CutOutcome::Cut(_) => break,
                CutOutcome::Inseparable => j0 = n,
                CutOutcome::Elsewhere(at) => {
                    let q = index.quantum_of(at, &self.keys[j0]);
                    while j0 < n && index.quantum_of(at, &self.keys[j0]) == q {
                        j0 += 1;
                    }
                }
            }
        }
        while i0 > 0 {
            let at = entry(index, self.first())?;
            let cut = Cut {
                first: self.first(),
                left_last: &self.keys[i0 - 1],
                right_first: self.first_of_span(i0),
                last: self.last_of_span(j0),
            };
            let stand_in = index
                .placeholder_at(at, 0)
                .ok_or(StorageError::Corrupt("a page without a range"))?;
            match index.cut(at, &cut, stand_in) {
                CutOutcome::Cut(_) => break,
                CutOutcome::Inseparable => i0 = 0,
                CutOutcome::Elsewhere(at) => {
                    let q = index.quantum_of(at, &self.keys[i0 - 1]);
                    while i0 > 0
                        && index.quantum_of(at, &self.keys[i0 - 1]) == q
                    {
                        i0 -= 1;
                    }
                }
            }
        }
        Ok((i0 > 0 || j0 < n).then_some((i0, j0)))
    }
}

/// The entry `key` finds.
fn entry(index: &Pages, key: &[u8]) -> StorageResult<Slot> {
    match index.find(key) {
        Found::Page { at, .. } | Found::Hole(at) => Ok(at),
        Found::Uncovered => Err(StorageError::Corrupt("a page lost its entry")),
    }
}
#[cfg(test)]
mod tests {
    use crate::StorageResult;
    use crate::dictionary::Dictionary;
    use crate::ordered::{Found, Placed, Slot};
    use crate::page::SlotPage;
    use crate::pages::{Pages, disk_pages};
    use crate::shape::{Rules, Shaped, shape};

    const SH: u64 = 0x5EED_0081_0005_0001;

    fn of(keys: &[[u8; 3]]) -> SlotPage {
        let mut p = SlotPage::new(SH);
        for k in keys {
            p.upsert(k, k.to_vec());
        }
        p
    }

    /// One page of two blocks at 10 holding `keys`, counted; a round of
    /// `records` put.
    fn round(keys: &[[u8; 3]], records: &[[u8; 3]]) -> (Pages, Shaped) {
        let puts: Vec<([u8; 3], bool)> =
            records.iter().map(|&k| (k, true)).collect();
        changes(keys, &puts)
    }

    /// As [`round`], the round putting (`true`) or removing each key.
    fn changes(
        keys: &[[u8; 3]],
        records: &[([u8; 3], bool)],
    ) -> (Pages, Shaped) {
        let mut ix = disk_pages(true);
        let page = Placed {
            address: 10,
            blocks: 2,
            meta: 0,
        };
        let at = ix.place(&keys[0], page).unwrap();
        ix.set_count(at, keys.len()).unwrap();
        let rules = Rules {
            target_blocks: 16,
            keep_holes: false,
            partial: true,
        };
        let held = of(keys);
        let load = |_| -> StorageResult<SlotPage> { Ok(held.clone()) };
        let records = records
            .iter()
            .map(|(k, put)| (k.to_vec(), put.then(|| k.to_vec())))
            .collect();
        let dict = Dictionary::new();
        let shaped = shape(&mut ix, records, &rules, SH, load, |p| {
            p.to_bytes(&dict, false)
        })
        .unwrap();
        (ix, shaped)
    }

    fn written(shaped: &Shaped) -> Vec<Vec<[u8; 3]>> {
        let key = |k: &[u8]| <[u8; 3]>::try_from(k).unwrap();
        shaped
            .pages
            .iter()
            .map(|p| {
                p.decoded
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|r| key(r.0))
                    .collect()
            })
            .collect()
    }

    /// The entry `key` finds — the new ones point at a stand-in until the
    /// window is placed, so entries, not addresses, tell pages apart.
    fn entry(ix: &Pages, key: &[u8]) -> Option<Slot> {
        match ix.find(key) {
            Found::Page { at, .. } => Some(at),
            _ => None,
        }
    }

    #[test]
    fn appends_past_a_page_leave_it_and_go_to_a_fragment() {
        let (ix, shaped) =
            round(&[[0, 1, 0], [0, 2, 0]], &[[0, 3, 0], [0, 4, 0]]);
        assert!(shaped.retired.is_empty(), "the page stays");
        assert_eq!(written(&shaped), [vec![[0, 3, 0], [0, 4, 0]]]);
        assert_eq!(entry(&ix, &[0, 1, 0]), entry(&ix, &[0, 2, 0]));
        assert_ne!(entry(&ix, &[0, 2, 0]), entry(&ix, &[0, 3, 0]));
    }

    #[test]
    fn a_fragment_takes_the_quantum_it_shares_with_the_page() {
        // Quanta are the first two bytes: 0x0002 holds the page's last two
        // keys and the first new one.
        let (ix, shaped) =
            round(&[[0, 1, 0], [0, 2, 0], [0, 2, 5]], &[[0, 2, 9], [0, 3, 0]]);
        assert!(shaped.retired.is_empty(), "the page stays");
        assert_eq!(
            written(&shaped),
            [vec![[0, 2, 0], [0, 2, 5], [0, 2, 9], [0, 3, 0]]]
        );
        assert_ne!(
            entry(&ix, &[0, 1, 0]),
            entry(&ix, &[0, 2, 0]),
            "the quantum is the fragment's now"
        );
        assert_eq!(entry(&ix, &[0, 2, 0]), entry(&ix, &[0, 3, 0]));
        // A list's length sums the counts: the page counts what it covers.
        let kept = entry(&ix, &[0, 1, 0]).unwrap();
        assert_eq!(ix.count_at(kept), Some(1));
    }

    #[test]
    fn a_change_inside_the_page_rewrites_it_with_the_appends() {
        let (_, shaped) =
            round(&[[0, 1, 0], [0, 2, 0]], &[[0, 1, 5], [0, 3, 0]]);
        assert_eq!(shaped.retired.len(), 1, "the page is rewritten");
        assert_eq!(
            written(&shaped),
            [vec![[0, 1, 0], [0, 1, 5], [0, 2, 0], [0, 3, 0]]]
        );
    }

    /// Nine keys a quantum apart.
    fn nine() -> Vec<[u8; 3]> {
        (1..=9).map(|i| [0, i, 0]).collect()
    }

    #[test]
    fn an_update_in_the_middle_writes_its_span_alone() {
        let (ix, shaped) = round(&nine(), &[[0, 5, 0]]);
        assert!(shaped.retired.is_empty(), "the page stays");
        assert_eq!(shaped.named.len(), 1, "named either side of the span");
        assert_eq!(written(&shaped), [vec![[0, 5, 0]]]);
        let (left, span, right) = (
            entry(&ix, &[0, 4, 0]),
            entry(&ix, &[0, 5, 0]),
            entry(&ix, &[0, 6, 0]),
        );
        assert_eq!(left, entry(&ix, &[0, 1, 0]));
        assert!(left != span && span != right && left != right);
        // Either side names the page still, counting what it covers of it.
        let page_at = |at: Slot| match ix.at(at) {
            Some(Found::Page { page, .. }) => page.address,
            other => panic!("{other:?}"),
        };
        assert_eq!(page_at(right.unwrap()), 10);
        assert_eq!(page_at(left.unwrap()), 10);
        assert_eq!(ix.count_at(left.unwrap()), Some(4));
        assert_eq!(ix.count_at(right.unwrap()), Some(4));
    }

    #[test]
    fn a_removal_in_the_middle_is_a_hole_over_the_page() {
        let (ix, shaped) = changes(&nine(), &[([0, 5, 0], false)]);
        assert!(shaped.retired.is_empty() && shaped.pages.is_empty());
        assert!(matches!(ix.find(&[0, 5, 0]), Found::Hole(_)));
        assert!(
            matches!(ix.find(&[0, 6, 0]), Found::Page { page, .. } if page.address == 10)
        );
    }

    #[test]
    fn a_change_to_most_of_a_page_rewrites_it() {
        let most: Vec<[u8; 3]> = (2..=8).map(|i| [0, i, 0]).collect();
        let (_, shaped) = round(&nine(), &most);
        assert_eq!(shaped.retired.len(), 1, "the page is rewritten");
        assert_eq!(shaped.named, Vec::new());
    }

    #[test]
    fn a_page_mostly_removed_is_rewritten() {
        let gone: Vec<([u8; 3], bool)> =
            (2..=7).map(|i| ([0, i, 0], false)).collect();
        let (_, shaped) = changes(&nine(), &gone);
        assert_eq!(shaped.retired.len(), 1, "the page is rewritten");
        assert_eq!(written(&shaped), [vec![[0, 1, 0], [0, 8, 0], [0, 9, 0]]]);
    }
}
