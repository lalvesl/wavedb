//! Test-only in-memory [`Store`] and tree-invariant helpers, shared by the
//! `tree*` module tests.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::error::Result;
use crate::id::Id;
use crate::local_id::LocalId;
use crate::store::{Store, Write};
use crate::u48::U48;

use super::node::NodeBody;
use super::tree::BpTree;

/// In-memory `Store` for exercising the index layer.
#[derive(Default)]
pub struct MemStore(Mutex<BTreeMap<u128, Vec<u8>>>);

impl MemStore {
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

/// Walk the whole tree, asserting the B+tree shape invariants:
///
/// - every leaf sits at the same depth;
/// - every **non-root** node holds at least ¼ of its capacity and at most all
///   of it;
/// - separators bound their subtrees (keys strictly ascending globally);
/// - every reachable node exists (no dangling pointers), and the store holds
///   **only** reachable nodes (no leaks).
///
/// Returns the keys in walk order.
pub(super) async fn check_invariants(
    tree: &BpTree,
    store: &MemStore,
    tenant: U48,
) -> Vec<u64> {
    let mut keys = Vec::new();
    let mut reachable = 0usize;
    let mut leaf_depth: Option<usize> = None;
    // (node, depth, is_root)
    let mut queue: Vec<(LocalId, usize, bool)> = vec![(tree.root(), 0, true)];

    while let Some((node_id, depth, is_root)) = queue.pop() {
        reachable += 1;
        let bytes = store
            .get(node_id.to_id(tenant))
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("dangling node pointer {node_id:?}"));
        match NodeBody::<LocalId>::from_bytes(&bytes).unwrap() {
            NodeBody::Leaf(node_keys) => {
                match leaf_depth {
                    None => leaf_depth = Some(depth),
                    Some(d) => {
                        assert_eq!(d, depth, "leaves at differing depths");
                    }
                }
                if !is_root {
                    assert!(
                        node_keys.len() >= tree.leaf_cap / 4,
                        "underfull leaf survived: {} < {}",
                        node_keys.len(),
                        tree.leaf_cap / 4
                    );
                }
                assert!(node_keys.len() <= tree.leaf_cap, "overfull leaf");
                keys.extend(node_keys.iter().map(|k| k.key()));
            }
            NodeBody::Internal { leftmost, entries } => {
                if !is_root {
                    assert!(
                        entries.len() >= tree.internal_cap / 4,
                        "underfull internal node survived"
                    );
                }
                assert!(
                    entries.len() <= tree.internal_cap,
                    "overfull internal"
                );
                assert!(
                    !entries.is_empty() || is_root,
                    "empty non-root internal"
                );
                // Reverse so the pop-order visits children left→right.
                for (_, child) in entries.iter().rev() {
                    queue.push((*child, depth + 1, false));
                }
                queue.push((leftmost, depth + 1, false));
            }
        }
    }

    // The walk visits left→right, so keys must come out strictly ascending.
    assert!(
        keys.windows(2).all(|w| w[0] < w[1]),
        "keys not strictly ascending"
    );
    assert_eq!(
        reachable,
        store.len(),
        "store holds unreachable nodes (leak) or misses reachable ones"
    );
    keys
}
