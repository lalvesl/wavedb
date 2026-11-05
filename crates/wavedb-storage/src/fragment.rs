//! Appending past a page without rewriting it ([RFC 0081] §2) — split from
//! [`crate::shape`], whose rounds route the records.
//!
//! A round whose records for a page all lie past its last key — a lane
//! keyed by time, a collection's tail in a lane of many — leaves the page as
//! it is, and writes those records as a **fragment**: a page of their own,
//! under a new entry cut from the page's at the quantum boundary between
//! them. The page written once is never written again for the appends that
//! follow it, which is what a durable round paid for most: the tail page,
//! rewritten whole, to carry a few hundred bytes.
//!
//! When the page's last key shares a quantum with the first new one, the
//! fragment takes that quantum: its keys from the page come along, and the
//! page keeps them as keys its entry no longer covers (read through its
//! entry, `pages::clip`).
//!
//! A page of one block is rewritten instead: the fragment would cost the
//! same block and an entry more.
//!
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use crate::error::{StorageError, StorageResult};
use crate::ordered::{Cut, CutOutcome, Found, Placed};
use crate::page::SlotPage;
use crate::pages::Pages;
use crate::shape::{Rules, Shaped, Staged, cut_down};

impl Staged {
    /// The stored page `placed`, read as `page`, staged unchanged — knowing
    /// its last key when it takes its appends as a fragment: a settle's
    /// round, and a page of more than one block.
    pub(crate) fn stored(
        placed: Placed<u8>,
        page: SlotPage,
        rules: &Rules,
    ) -> Self {
        let last = (rules.fragments && placed.blocks > 1)
            .then(|| page.iter().last().map(|(k, _)| k.to_vec()))
            .flatten();
        Self {
            old: Some(placed),
            page,
            last,
            tail: Vec::new(),
            touched: false,
        }
    }
}

/// Write `st`'s appends as a fragment, if they are its only change and its
/// keys part from them: `None` then, the page kept as it is. Otherwise `st`
/// with its appends in its page, to be rewritten whole.
///
/// # Errors
/// An entry lost; the encoder's faults.
pub fn appended<E>(
    index: &mut Pages,
    mut st: Staged,
    rules: &Rules,
    encode: &mut E,
    shaped: &mut Shaped,
) -> StorageResult<Option<Staged>>
where
    E: FnMut(&mut SlotPage) -> StorageResult<Vec<u8>>,
{
    let (Some(first), Some(last)) = (st.tail.first(), st.tail.last()) else {
        // A page the round left as it was — a removal of a key it does not
        // hold — is not written again.
        let unchanged = !st.touched && st.old.is_some();
        return Ok((!unchanged).then_some(st));
    };
    let keys: Vec<Vec<u8>> = st.page.iter().map(|(k, _)| k.to_vec()).collect();
    let parted = if st.touched || keys.is_empty() {
        None
    } else {
        parted(index, &keys, &first.0, &last.0)?
    };
    let Some(from) = parted else {
        for (key, bytes) in std::mem::take(&mut st.tail) {
            st.page.upsert(&key, bytes);
        }
        return Ok(Some(st));
    };
    let mut fragment = SlotPage::new(st.page.struct_hash());
    for (key, bytes) in st.page.iter().skip(from) {
        fragment.upsert(key, bytes.to_vec());
    }
    for (key, bytes) in st.tail {
        fragment.upsert(&key, bytes);
    }
    if from < keys.len() {
        // The page holds fewer keys under its entry now.
        let Found::Page { at, .. } = index.find(&keys[0]) else {
            return Err(StorageError::Corrupt("a kept page lost its entry"));
        };
        index.set_count(at, from);
    }
    cut_down(index, fragment, rules, encode, shaped)?;
    Ok(None)
}

/// Cut the entry of the page holding `keys` for a fragment of the keys from
/// `first` to `last`, past them: where the fragment starts among `keys` —
/// all of them stay when the cut falls between the page's last key and
/// `first`, and those of its last quantum go along when they share it.
/// `None` when no cut parts them.
fn parted(
    index: &mut Pages,
    keys: &[Vec<u8>],
    first: &[u8],
    last: &[u8],
) -> StorageResult<Option<usize>> {
    let Found::Page { at, .. } = index.find(&keys[0]) else {
        return Err(StorageError::Corrupt(
            "a page to append to lost its entry",
        ));
    };
    let stand_in = |index: &Pages, at| {
        index
            .placeholder_at(at, 0)
            .ok_or(StorageError::Corrupt("a page without a range"))
    };
    let mut cut = Cut {
        first: &keys[0],
        left_last: &keys[keys.len() - 1],
        right_first: first,
        last,
    };
    let right = stand_in(index, at)?;
    let at = match index.cut(at, &cut, right) {
        CutOutcome::Cut(_) => return Ok(Some(keys.len())),
        CutOutcome::Inseparable => return Ok(None),
        CutOutcome::Elsewhere(at) => at,
    };
    let q = |k: &[u8]| index.quantum_of(at, k);
    let shared = q(first);
    let Some(i) = keys.iter().position(|k| q(k) == shared).filter(|&i| i > 0)
    else {
        return Ok(None);
    };
    cut.left_last = &keys[i - 1];
    cut.right_first = &keys[i];
    let right = stand_in(index, at)?;
    Ok(matches!(index.cut(at, &cut, right), CutOutcome::Cut(_)).then_some(i))
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
    /// `records`.
    fn round(keys: &[[u8; 3]], records: &[[u8; 3]]) -> (Pages, Shaped) {
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
            fragments: true,
        };
        let held = of(keys);
        let load = |_| -> StorageResult<SlotPage> { Ok(held.clone()) };
        let records = records
            .iter()
            .map(|k| (k.to_vec(), Some(k.to_vec())))
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
}
