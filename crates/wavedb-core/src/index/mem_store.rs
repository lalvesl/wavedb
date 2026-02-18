//! Test-only in-memory [`Store`], shared by the index and collection tests.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::error::Result;
use crate::id::Id;
use crate::store::{Store, Write};

/// In-memory `Store` for exercising the index layer — counting the batches
/// asked durable, which it applies like any other.
#[derive(Default)]
pub struct MemStore(Mutex<BTreeMap<u128, Vec<u8>>>, Mutex<usize>);

impl MemStore {
    /// How many batches came through `apply_durable`.
    pub fn durable_batches(&self) -> usize {
        *self.1.lock().unwrap()
    }

    /// Number of stored values (in tree-only tests: the live node count).
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }

    /// Every stored `(Id, bytes)` pair — byte-level comparisons between two
    /// stores (mirror-fidelity tests).
    pub fn entries(&self) -> Vec<(Id, Vec<u8>)> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|(raw, bytes)| (Id::from_raw(*raw), bytes.clone()))
            .collect()
    }
}

impl Store for MemStore {
    fn get(&self, id: Id) -> impl Future<Output = Result<Option<Vec<u8>>>> {
        std::future::ready(Ok(self.0.lock().unwrap().get(&id.raw()).cloned()))
    }

    fn apply(&self, batch: &[Write]) -> impl Future<Output = Result<()>> {
        std::future::ready(self.apply_now(batch))
    }

    fn apply_durable(
        &self,
        batch: &[Write],
    ) -> impl Future<Output = Result<()>> {
        *self.1.lock().unwrap() += 1;
        std::future::ready(self.apply_now(batch))
    }
}

impl MemStore {
    /// The batch, applied synchronously.
    fn apply_now(&self, batch: &[Write]) -> Result<()> {
        let mut m = self.0.lock().unwrap();
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
                Write::Expect(..) => {}
            }
        }
        drop(m);
        Ok(())
    }
}
