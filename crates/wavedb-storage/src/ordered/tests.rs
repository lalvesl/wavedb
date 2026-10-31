//! The index against a model lane: pages as sorted key lists at fake
//! addresses, written copy-on-write, cut when full, and every key looked up
//! again after each change.

use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::key::cmp_padded;
use super::*;

fn shape(
    key_shift: u8,
    address_bits: u8,
    address_factor: u8,
    size_factor: u8,
) -> Shape {
    Shape::new(key_shift, address_bits, address_factor, size_factor).unwrap()
}

/// Ranges sorted and disjoint by start; every range non-empty, its first
/// entry at quantum 0 and the rest ascending.
fn check<M: Copy + Default + std::fmt::Debug>(ix: &PageIndex<M>) {
    for pair in ix.ranges.windows(2) {
        let order = cmp_padded(&pair[0].head.start(), &pair[1].head.start());
        assert_eq!(
            order,
            Ordering::Less,
            "ranges out of order: {:?} then {:?}",
            pair[0],
            pair[1]
        );
    }
    for r in &ix.ranges {
        let qs: Vec<u16> = r.entries.pages.iter().map(|&e| q_of(e)).collect();
        assert_eq!(r.entries.pages.len(), r.entries.meta.len());
        assert_eq!(qs.first(), Some(&0), "a range starts at quantum 0");
        assert!(qs.windows(2).all(|w| w[0] < w[1]), "quanta ascend");
    }
}

/// xorshift64: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| self.next() as u8).collect()
    }
}

/// A lane over the index: its pages' keys by address.
struct Lane {
    ix: PageIndex<()>,
    pages: BTreeMap<u64, Vec<Vec<u8>>>,
    next: u64,
    cap: usize,
    max_blocks: u64,
    rng: Rng,
}

impl Lane {
    fn new(seed: Shape, cap: usize, max_blocks: u64) -> Self {
        Self {
            ix: PageIndex::new(seed),
            pages: BTreeMap::new(),
            next: 0,
            cap,
            max_blocks,
            rng: Rng(0x9E37_79B9_7F4A_7C15),
        }
    }

    /// A fresh address, now and then far from the last one.
    fn alloc(&mut self) -> Placed<()> {
        let blocks = 1 + self.rng.below(self.max_blocks);
        if self.rng.below(10) == 0 {
            self.next += 100_000;
        }
        let address = self.next;
        self.next += blocks;
        Placed {
            address,
            blocks,
            meta: (),
        }
    }

    fn insert(&mut self, key: Vec<u8>) {
        match self.ix.find(&key) {
            Found::Uncovered => {
                let page = self.alloc();
                self.ix.place(&key, page).unwrap();
                self.pages.insert(page.address, vec![key]);
            }
            Found::Hole(at) => {
                let page = self.alloc();
                self.ix.retarget(at, page).unwrap();
                self.pages.insert(page.address, vec![key]);
            }
            Found::Page { at, page } => {
                let mut keys = self.pages.remove(&page.address).unwrap();
                if let Err(i) = keys.binary_search_by(|k| cmp_padded(k, &key)) {
                    keys.insert(i, key);
                }
                let left = self.alloc();
                let at = self.ix.retarget(at, left).unwrap();
                if keys.len() > self.cap {
                    self.split(at, left, &keys);
                } else {
                    self.pages.insert(left.address, keys);
                }
            }
        }
    }

    fn split(&mut self, at: Slot, left: Placed<()>, keys: &[Vec<u8>]) {
        let right = self.alloc();
        let cut = |i: usize| Cut {
            first: &keys[0],
            left_last: &keys[i - 1],
            right_first: &keys[i],
            last: &keys[keys.len() - 1],
        };
        let mid = keys.len() / 2;
        let i = match self.ix.cut(at, &cut(mid), right) {
            CutOutcome::Cut(_) => mid,
            CutOutcome::Elsewhere(at) => {
                let q = |k: &[u8]| self.ix.quantum_of(at, k).unwrap();
                let parts = |i: &usize| q(&keys[i - 1]) < q(&keys[*i]);
                let i = (1..keys.len())
                    .filter(parts)
                    .min_by_key(|i| i.abs_diff(mid))
                    .unwrap();
                assert!(matches!(
                    self.ix.cut(at, &cut(i), right),
                    CutOutcome::Cut(_)
                ));
                i
            }
            CutOutcome::Inseparable => panic!("distinct keys are separable"),
        };
        self.pages.insert(left.address, keys[..i].to_vec());
        self.pages.insert(right.address, keys[i..].to_vec());
    }

    fn remove(&mut self, key: &[u8]) {
        let Found::Page { at, page } = self.ix.find(key) else {
            panic!("a stored key has a page");
        };
        let mut keys = self.pages.remove(&page.address).unwrap();
        keys.retain(|k| cmp_padded(k, key) != Ordering::Equal);
        if keys.is_empty() {
            self.ix.remove(at).unwrap();
        } else {
            let moved = self.alloc();
            self.ix.retarget(at, moved).unwrap();
            self.pages.insert(moved.address, keys);
        }
    }

    /// Every stored key is found in the page holding it, and every page is
    /// named by exactly one entry.
    fn verify(&self) {
        check(&self.ix);
        let mut named = 0;
        for (address, keys) in &self.pages {
            for key in keys {
                match self.ix.find(key) {
                    Found::Page { page, .. } => {
                        assert_eq!(page.address, *address);
                    }
                    other => panic!("key lost: {other:?}"),
                }
            }
            named += 1;
        }
        let pages = self
            .ix
            .ranges
            .iter()
            .flat_map(|r| r.entries.pages.iter().map(|&e| r.head.decode(e)))
            .flatten()
            .count();
        assert_eq!(pages, named, "one entry per page");
    }

    fn run(&mut self, keys: &[Vec<u8>], removals: usize) {
        for (n, key) in keys.iter().enumerate() {
            self.insert(key.clone());
            if n % 97 == 0 {
                self.verify();
            }
        }
        self.verify();
        for key in keys.iter().take(removals) {
            self.remove(key);
        }
        self.verify();
        for key in keys.iter().take(removals) {
            assert!(!matches!(
                self.ix.find(key),
                Found::Page { page, .. } if self.pages[&page.address].contains(key)
            ));
        }
    }
}

fn time_keys(rng: &mut Rng, n: usize) -> Vec<Vec<u8>> {
    let mut t = 1_700_000_000_000_000_000u64;
    (0..n)
        .map(|_| {
            t += rng.below(2_000_000);
            let mut k = t.to_be_bytes().to_vec();
            k.extend(rng.bytes(10));
            k
        })
        .collect()
}

#[test]
fn a_time_stream_keeps_every_key_findable() {
    let mut rng = Rng(7);
    let keys = time_keys(&mut rng, 4000);
    let mut lane = Lane::new(shape(21, 6, 1, 1), 16, 3);
    lane.run(&keys, 1500);
}

#[test]
fn uniform_keys_keep_every_key_findable() {
    let mut rng = Rng(11);
    let keys: Vec<_> = (0..4000).map(|_| rng.bytes(16)).collect();
    let mut lane = Lane::new(shape(48, 12, 1, 1), 16, 3);
    lane.run(&keys, 1500);
}

#[test]
fn a_hot_value_goes_deeper_than_its_prefix() {
    let mut rng = Rng(13);
    let keys: Vec<_> = (0..3000u64)
        .map(|i| {
            let category: u8 = if rng.below(10) == 0 {
                rng.below(4) as u8
            } else {
                1
            };
            let mut k = vec![b'c', b'a', b't', category];
            k.extend(i.to_be_bytes());
            k.extend(rng.bytes(10));
            k
        })
        .collect();
    let mut lane = Lane::new(shape(40, 10, 1, 1), 12, 2);
    lane.run(&keys, 1000);
    assert!(lane.ix.sub_headers().any(|h| h.depth() >= 8));
}

#[test]
fn strings_with_an_anchor_keep_every_key_findable() {
    let mut rng = Rng(17);
    let keys: Vec<_> = (0..3000)
        .map(|_| {
            let len = 1 + rng.below(12) as usize;
            let mut k: Vec<u8> =
                (0..len).map(|_| b'a' + rng.below(4) as u8).collect();
            k.push(0);
            k.extend(rng.bytes(10));
            k
        })
        .collect();
    let mut lane = Lane::new(shape(32, 14, 1, 1), 12, 2);
    lane.run(&keys, 1000);
}

#[test]
fn pages_larger_than_the_size_field_get_a_range_of_their_own() {
    let mut rng = Rng(19);
    let keys = time_keys(&mut rng, 2000);
    // One size bit: a page past two blocks needs a larger unit.
    let mut lane = Lane::new(shape(20, 15, 1, 1), 16, 9);
    lane.run(&keys, 500);
    assert!(lane.ix.sub_headers().any(|h| h.shape.size_factor() > 1));
}

#[test]
fn a_rewrite_outside_the_window_isolates_the_page() {
    let mut ix = PageIndex::new(shape(8, 4, 1, 1));
    let page = |address| Placed {
        address,
        blocks: 1,
        meta: (),
    };
    let at = ix.place(&[0, 0, 0, 0, 0, 0, 1, 0], page(10)).unwrap();
    assert_eq!(ix.sub_headers().count(), 1);
    // Fifteen units past the base is the hole marker: out of the window.
    let at = ix.retarget(at, page(10 + 15)).unwrap();
    assert_eq!(ix.sub_headers().count(), 1);
    assert_eq!(ix.sub_headers().next().unwrap().base_block, 25);
    assert!(matches!(
        ix.find(&[0, 0, 0, 0, 0, 0, 1, 0]),
        Found::Page {
            page: Placed { address: 25, .. },
            ..
        }
    ));
    assert_eq!(
        ix.at(at).map(|f| matches!(f, Found::Page { .. })),
        Some(true)
    );
}

#[test]
fn equal_keys_are_inseparable() {
    let mut ix = PageIndex::new(shape(0, 8, 1, 1));
    let page = |address| Placed {
        address,
        blocks: 1,
        meta: (),
    };
    let at = ix.place(b"ab", page(0)).unwrap();
    let cut = Cut {
        first: b"ab",
        left_last: b"ab",
        right_first: b"ab\0",
        last: b"ab\0",
    };
    assert_eq!(ix.cut(at, &cut, page(1)), CutOutcome::Inseparable);
}

#[test]
fn removing_the_first_page_hands_its_quanta_on() {
    let mut ix = PageIndex::new(shape(0, 8, 1, 1));
    let page = |address| Placed {
        address,
        blocks: 1,
        meta: (),
    };
    let key = |b: u8| [0, 0, 0, 0, 0, 0, 0, b];
    let at = ix.place(&key(0), page(0)).unwrap();
    let cut = Cut {
        first: &key(0),
        left_last: &key(0),
        right_first: &key(5),
        last: &key(5),
    };
    assert!(matches!(ix.cut(at, &cut, page(1)), CutOutcome::Cut(_)));
    ix.remove(at).unwrap();
    assert!(matches!(
        ix.find(&key(0)),
        Found::Page {
            page: Placed { address: 1, .. },
            ..
        }
    ));
    check(&ix);
}
