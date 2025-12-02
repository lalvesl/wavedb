//! The write half of `#[wavedb::fuzzy]` ([RFC 0056]): keeping each posting
//! chain in step with the record, inside the collection's one atomic batch.
//!
//! ## The rule a declared list could not have
//!
//! A posting carries a gram, a length and an anchor — **no record bytes**. So
//! the posting set is a pure function of the indexed field, and that buys the
//! rule [`plan_fuzzy_moves`] is built around:
//!
//! > A save whose indexed field did not change writes **nothing** here.
//!
//! RFC 0051's lists must rewrite the record in every list unconditionally,
//! because a list *duplicates the record* and any other field moving makes the
//! copy stale. Nothing is duplicated here, so there is nothing to go stale, and
//! when the field does change only the **symmetric difference** of the two gram
//! sets moves — a one-character edit touches a handful of keys, not the
//! `L + n - 1` a remove-and-reinsert would.
//!
//! ## The cost, stated plainly
//!
//! Inserting a record writes `L + n - 1` keys scattered across the key space,
//! so it touches up to that many pages of the posting lane — a 20-character
//! name is ~22 entries (RFC 0075, RFC 0078 §7). Removal is symmetric. It is
//! one barrier, because it rides the collection's single batch, but it is
//! not one page. That is the honest
//! headline of this feature the way "one full copy of every record per
//! declaration" is RFC 0051's.
//!
//! [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md

use crate::collection::Collection;
use crate::fuzzy::{field_key, gram_prefixes, normalize};
use crate::id::Id;
use crate::index::{Chain, SecKey};
use crate::local_id::LocalId;
use crate::store::Write;
use crate::traits::NonUniqueStruct;

impl<T: NonUniqueStruct> Collection<T> {
    /// Every posting key fuzzy index `i` files `value` under.
    ///
    /// The length stored in each key is the **normalized** length, not the
    /// input's — folding can change it (`ß` becomes two characters), and the
    /// length filter compares against what the query normalizes to.
    pub(crate) fn fuzzy_keys(value: &T, i: usize, id: Id) -> Vec<SecKey> {
        let (n, fold) = T::fuzzy_profile(i);
        let text = normalize(value.fuzzy_source(i), fold);
        let rec = LocalId::from_id(id);
        gram_prefixes(&text, n)
            .into_iter()
            .map(|prefix| SecKey {
                field: field_key(&prefix, text.len()),
                rec,
            })
            .collect()
    }

    /// File a freshly inserted record in every fuzzy index.
    pub(crate) fn plan_fuzzy_inserts(
        &self,
        batch: &mut Vec<Write>,
        id: Id,
        value: &T,
    ) {
        for (i, chain) in self.fuzzy_chains().iter().enumerate() {
            for key in Self::fuzzy_keys(value, i, id) {
                batch.push(chain.insert(&key, &()));
            }
        }
    }

    /// Re-file a saved record: only the grams that actually moved.
    ///
    /// An unchanged indexed field yields two identical sets and therefore an
    /// empty difference both ways — no writes emitted. That is the whole
    /// reason a posting holds no record bytes.
    pub(crate) fn plan_fuzzy_moves(
        batch: &mut Vec<Write>,
        chains: &[Chain<()>],
        id: Id,
        values: (&T, &T),
    ) {
        let (old_value, value) = values;
        for (i, chain) in chains.iter().enumerate() {
            let old: Vec<SecKey> = Self::fuzzy_keys(old_value, i, id);
            let new: Vec<SecKey> = Self::fuzzy_keys(value, i, id);
            for key in old.iter().filter(|k| !new.contains(k)) {
                batch.push(chain.remove(key));
            }
            for key in new.iter().filter(|k| !old.contains(k)) {
                batch.push(chain.insert(key, &()));
            }
        }
    }

    /// Take a removed record out of every fuzzy index.
    pub(crate) fn plan_fuzzy_removes(
        &self,
        batch: &mut Vec<Write>,
        id: Id,
        value: &T,
    ) {
        for (i, chain) in self.fuzzy_chains().iter().enumerate() {
            for key in Self::fuzzy_keys(value, i, id) {
                batch.push(chain.remove(&key));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use crate::collection::Collection;
    use crate::fuzzy::{Fold, gram_prefixes, normalize};
    use crate::index::mem_store::MemStore;
    use crate::index::{Chain, Pivot, SecKey};
    use crate::local_id::LocalId;
    use crate::permission::PermissionRef;
    use crate::traits::{NonUniqueStruct, Shape, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::WaveWire;

    // What `#[wavedb(NonUnique)]` with one `#[wavedb::fuzzy]` on `name`
    // generates (core cannot use the proc-macro).
    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Member {
        name: String,
        city: String,
    }

    impl WaveDbStruct for Member {
        const STRUCT_HASH: u64 = 0x0F0_0001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct MemberPivot {
        permission: Option<PermissionRef>,
    }

    impl Pivot for MemberPivot {
        const STRUCT_HASH: u64 = 0x0F0_0002;
        fn permission(&self) -> Option<&PermissionRef> {
            self.permission.as_ref()
        }
    }

    impl NonUniqueStruct for Member {
        type Pivot = MemberPivot;
        const NUM_FUZZY: usize = 1;
        fn fuzzy_source(&self, index: usize) -> &str {
            match index {
                0 => self.name.as_str(),
                _ => "",
            }
        }
    }

    fn tenant() -> U48 {
        U48::new(11).unwrap()
    }

    fn member(name: &str) -> Member {
        Member {
            name: name.into(),
            city: "lisbon".into(),
        }
    }

    /// Every anchor `chain` files under a key starting with `prefix`.
    async fn under(
        store: &MemStore,
        chain: Chain<()>,
        prefix: &[u8],
    ) -> Vec<LocalId> {
        use futures::TryStreamExt;
        let from = SecKey {
            field: prefix.to_vec(),
            rec: LocalId::default(),
        };
        chain
            .scan_from(store, &from)
            .try_take_while(|(k, ())| {
                futures::future::ready(Ok(k.field.starts_with(prefix)))
            })
            .map_ok(|(k, ())| k.rec)
            .try_collect()
            .await
            .unwrap()
    }

    /// Every posting `chain` holds, in order.
    async fn all_keys(store: &MemStore, chain: Chain<()>) -> Vec<SecKey> {
        use futures::TryStreamExt;
        let from = SecKey {
            field: Vec::new(),
            rec: LocalId::default(),
        };
        chain
            .scan_from(store, &from)
            .map_ok(|(k, ())| k)
            .try_collect()
            .await
            .unwrap()
    }

    /// Every anchor the posting chain files under any gram of `query`.
    async fn candidates(
        store: &MemStore,
        col: &Collection<Member>,
        query: &str,
    ) -> Vec<LocalId> {
        let chain = col.fuzzy_chains().pop().unwrap();
        let (n, fold) = Member::fuzzy_profile(0);
        let mut out = Vec::new();
        for prefix in gram_prefixes(&normalize(query, fold), n) {
            out.extend(under(store, chain, &prefix).await);
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// How many postings the chain holds in total.
    async fn postings(store: &MemStore, col: &Collection<Member>) -> usize {
        let chain = col.fuzzy_chains().pop().unwrap();
        under(store, chain, &[]).await.len()
    }

    // The whole point of the index: a typo still finds the record. This is the
    // *candidate* stage — exactness comes from verifying survivors — so what it
    // proves is that the postings are there to be found at all.
    #[test]
    fn a_misspelling_still_reaches_the_record() {
        block_on(async {
            let store = MemStore::default();
            let pivot = Collection::<Member>::create(&store, tenant())
                .await
                .unwrap();
            let col = Collection::<Member>::at(pivot, tenant());

            let ada = col.insert(&store, &member("John Smith")).await.unwrap();
            col.insert(&store, &member("Zebediah Q")).await.unwrap();

            let hits = candidates(&store, &col, "jhon smtih").await;
            assert!(
                hits.contains(&LocalId::from_id(ada)),
                "a two-typo query lost the record entirely"
            );
        });
    }

    // The rule RFC 0056 builds the whole write path around, and the one a
    // declared list cannot have: a posting holds no record bytes, so a save
    // that leaves the indexed field alone has nothing to say to this index.
    #[test]
    fn a_save_that_leaves_the_field_alone_writes_no_postings() {
        block_on(async {
            let store = MemStore::default();
            let pivot = Collection::<Member>::create(&store, tenant())
                .await
                .unwrap();
            let col = Collection::<Member>::at(pivot, tenant());
            let id = col.insert(&store, &member("John Smith")).await.unwrap();

            // Asked of the planner directly, because that is exactly the
            // claim: not "the postings end up the same" (they would either
            // way) but "no write was planned at all".
            let chains = col.fuzzy_chains();
            let mut batch = Vec::new();
            let same_name = Member {
                name: "John Smith".into(),
                city: "porto".into(),
            };
            Collection::<Member>::plan_fuzzy_moves(
                &mut batch,
                &chains,
                id,
                (&member("John Smith"), &same_name),
            );
            assert!(
                batch.is_empty(),
                "a save that changed no gram planned {} writes",
                batch.len()
            );

            // And the same through the real save path: the index is untouched.
            let before = postings(&store, &col).await;
            col.save(&store, id, &same_name).await.unwrap();
            assert_eq!(postings(&store, &col).await, before);
            assert!(
                candidates(&store, &col, "John Smith")
                    .await
                    .contains(&LocalId::from_id(id))
            );
        });
    }

    // The other half of the same rule: a changed field moves only the
    // **symmetric difference**, not every gram. `"John Smith"` → `"John Smyth"`
    // is one edit, so the vast majority of the postings must stay put.
    #[test]
    fn a_one_character_edit_moves_only_a_handful_of_postings() {
        block_on(async {
            let store = MemStore::default();
            let pivot = Collection::<Member>::create(&store, tenant())
                .await
                .unwrap();
            let col = Collection::<Member>::at(pivot, tenant());
            let id = col.insert(&store, &member("John Smith")).await.unwrap();

            let chains = col.fuzzy_chains();
            let before = all_keys(&store, chains[0]).await;
            let mut batch = Vec::new();
            Collection::<Member>::plan_fuzzy_moves(
                &mut batch,
                &chains,
                id,
                (&member("John Smith"), &member("John Smyth")),
            );

            crate::store::Store::apply(&store, &batch).await.unwrap();
            let after = all_keys(&store, chains[0]).await;

            // One substitution disrupts at most `n` grams either side, so the
            // moved set is bounded by ~2n — nowhere near the `L + n - 1` a
            // remove-and-reinsert would touch.
            let moved = before.iter().filter(|k| !after.contains(k)).count()
                + after.iter().filter(|k| !before.contains(k)).count();
            assert!(
                moved > 0 && moved < before.len(),
                "one edit moved {moved} of {} postings — the difference \
                 should be a handful, not the whole set",
                before.len()
            );
        });
    }

    // …and when the field does change, only the difference moves — not a full
    // remove-and-reinsert of every gram.
    #[test]
    fn a_renamed_record_is_found_under_the_new_name_only() {
        block_on(async {
            let store = MemStore::default();
            let pivot = Collection::<Member>::create(&store, tenant())
                .await
                .unwrap();
            let col = Collection::<Member>::at(pivot, tenant());
            let id = col.insert(&store, &member("John Smith")).await.unwrap();

            col.save(&store, id, &member("Jane Doe")).await.unwrap();

            let anchor = LocalId::from_id(id);
            assert!(
                candidates(&store, &col, "Jane Doe").await.contains(&anchor)
            );
            // "John Smith" and "Jane Doe" share a few grams (" j", "j", …), so
            // the old name still produces *some* candidates — the count filter
            // and the verify step are what reject them. What must be gone is
            // the record's posting under a gram unique to the old name.
            let dropped = gram_prefixes(&normalize("smi", Fold::Latin), 3);
            let chain = col.fuzzy_chains().pop().unwrap();
            for prefix in dropped {
                let hits = under(&store, chain, &prefix).await;
                assert!(
                    !hits.contains(&anchor),
                    "the old name's grams still point at the record"
                );
            }
        });
    }

    // A removed record leaves no postings behind: the index holds the living
    // set, the same as every list and secondary.
    #[test]
    fn a_removal_takes_every_posting_with_it() {
        block_on(async {
            let store = MemStore::default();
            let pivot = Collection::<Member>::create(&store, tenant())
                .await
                .unwrap();
            let col = Collection::<Member>::at(pivot, tenant());
            let id = col.insert(&store, &member("John Smith")).await.unwrap();
            assert!(postings(&store, &col).await > 0);

            assert!(col.remove(&store, id).await.unwrap());
            assert_eq!(
                postings(&store, &col).await,
                0,
                "a removed record left postings pointing at it"
            );
        });
    }
}
