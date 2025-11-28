//! Test-only in-memory [`Store`], shared by the index and collection tests.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::error::Result;
use crate::id::Id;
use crate::store::{Entry, Span, Store, Write, read_count, read_span};

/// Every ordered lane, by lane hash.
type Lanes = BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>;

/// In-memory `Store` for exercising the index layer — counting the batches
/// asked durable, which it applies like any other.
#[derive(Default)]
pub struct MemStore {
    records: Mutex<BTreeMap<u128, Vec<u8>>>,
    lanes: Mutex<Lanes>,
    durable: Mutex<usize>,
}

impl MemStore {
    /// How many batches came through `apply_durable`.
    pub fn durable_batches(&self) -> usize {
        *self.durable.lock().unwrap()
    }

    /// Number of stored values — records and lane entries.
    pub fn len(&self) -> usize {
        self.records.lock().unwrap().len()
            + self
                .lanes
                .lock()
                .unwrap()
                .values()
                .map(BTreeMap::len)
                .sum::<usize>()
    }

    /// Every stored `(Id, bytes)` pair — byte-level comparisons between two
    /// stores (mirror-fidelity tests).
    pub fn entries(&self) -> Vec<(Id, Vec<u8>)> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .map(|(raw, bytes)| (Id::from_raw(*raw), bytes.clone()))
            .collect()
    }
}

impl Store for MemStore {
    fn get(&self, id: Id) -> impl Future<Output = Result<Option<Vec<u8>>>> {
        std::future::ready(Ok(self
            .records
            .lock()
            .unwrap()
            .get(&id.raw())
            .cloned()))
    }

    fn scan(
        &self,
        lane: u64,
        span: &Span,
    ) -> impl Future<Output = Result<Vec<Entry>>> {
        let lanes = self.lanes.lock().unwrap();
        std::future::ready(Ok(lanes
            .get(&lane)
            .map_or_else(Vec::new, |l| read_span(l, span))))
    }

    fn count(
        &self,
        lane: u64,
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> impl Future<Output = Result<u64>> {
        let lanes = self.lanes.lock().unwrap();
        std::future::ready(Ok(lanes
            .get(&lane)
            .map_or(0, |l| read_count(l, lo, hi))))
    }

    fn apply(&self, batch: &[Write]) -> impl Future<Output = Result<()>> {
        std::future::ready(self.apply_now(batch))
    }

    fn apply_durable(
        &self,
        batch: &[Write],
    ) -> impl Future<Output = Result<()>> {
        *self.durable.lock().unwrap() += 1;
        std::future::ready(self.apply_now(batch))
    }
}

impl MemStore {
    /// The batch, applied synchronously.
    fn apply_now(&self, batch: &[Write]) -> Result<()> {
        let mut m = self.records.lock().unwrap();
        let mut lanes = self.lanes.lock().unwrap();
        // Guards validate against the pre-batch state; any mismatch refuses
        // the whole batch before a single write lands.
        for w in batch {
            if let Write::Expect(_, id, expected) = w
                && m.get(&id.raw()) != expected.as_ref()
            {
                return Err(crate::error::Error::Conflict(*id));
            }
        }
        for w in batch {
            match w {
                Write::Put(id, b) => {
                    m.insert(id.raw(), b.clone());
                }
                Write::Remove(_, id) => {
                    m.remove(&id.raw());
                }
                Write::Set(lane, key, value) => {
                    lanes
                        .entry(*lane)
                        .or_default()
                        .insert(key.clone(), value.clone());
                }
                Write::Unset(lane, key) => {
                    if let Some(l) = lanes.get_mut(lane) {
                        l.remove(key);
                    }
                }
                Write::Expect(..) => {}
            }
        }
        drop(m);
        drop(lanes);
        Ok(())
    }
}
