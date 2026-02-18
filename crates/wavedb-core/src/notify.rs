//! Mutation notifications — the core seam live sync (M7) hangs off.
//!
//! Every mutating op is exactly **one atomic [`Store::apply`] batch**;
//! immediately after that apply resolves, the write path hands the store a
//! [`Mutation`] via [`Store::note_mutation`] — the op-level meaning a raw
//! batch cannot carry (a chained save also writes archive records
//! metadata-indistinguishable from the live one, and a remove may rewrite
//! no record at all — only chain segments).
//!
//! The default `note_mutation` does nothing and never even builds the
//! value, so every ordinary store — the engine under a client cache, the
//! browser store, test stores — pays nothing. The node wraps its engine in
//! a store that overrides it and routes events to subscribers (M7's
//! declared-subscription push). Because the seam sits on the **write
//! paths**, it sees every mutation regardless of who drove it: a wire
//! command's generated step, a `#[server]` function body, or node-side
//! seeding. Mirror writes into a cache ([`Collection::adopt`]) run against
//! cache stores, whose default no-op makes them invisible — a mirrored
//! record is not a new mutation.
//!
//! [`Store::apply`]: crate::Store::apply
//! [`Store::note_mutation`]: crate::Store::note_mutation
//! [`Collection::adopt`]: crate::Collection::adopt

use crate::id::Id;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::u48::U48;

/// What a mutation did to its record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    /// Inserted, updated, or Unique-saved — [`Mutation::body`] is the new
    /// state.
    Saved,
    /// Moved to the dead tree — [`Mutation::body`] is empty. Carries the
    /// removal instant the dead-log entry is keyed by, so a watcher's
    /// catch-up cursor can advance past the removal.
    Removed(u64),
}

/// One committed mutation, as the write path that planned it saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mutation {
    /// The mutated `#[wavedb]` type.
    pub struct_hash: u64,
    /// The tenant whose data changed — subscription routing never crosses
    /// it.
    pub tenant: U48,
    /// The owning collection `Pivot` (`None` = a Unique anchor).
    pub pivot: Option<LocalId>,
    /// The record's stable identity (the anchor for Unique).
    pub id: Id,
    /// Saved or removed.
    pub kind: MutationKind,
    /// The new live version's [`Metadata`] (`None` on a removal — a remove
    /// rewrites no record). Riding the event lets a mirror adopt the
    /// node's authoritative chain data instead of minting its own.
    pub meta: Option<Metadata>,
    /// The record's new body wire bytes (empty for a removal).
    pub body: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use futures::executor::block_on;

    use super::{Mutation, MutationKind};
    use crate::collection::Collection;
    use crate::error::Result;
    use crate::id::Id;
    use crate::index::mem_store::MemStore;
    use crate::index::{ChainRoots, LogRoots, Pivot, Roots};
    use crate::permission::PermissionRef;
    use crate::record::save_unique;
    use crate::store::{Store, Write};
    use crate::traits::{NonUniqueStruct, Shape, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::{WaveWire, to_wire};

    /// A store overriding `note_mutation` — what the node's event-routing
    /// wrapper does, minus the routing.
    #[derive(Default)]
    struct Recording {
        inner: MemStore,
        noted: RefCell<Vec<Mutation>>,
    }

    impl Store for Recording {
        async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
            self.inner.get(id).await
        }
        async fn apply(&self, batch: &[Write]) -> Result<()> {
            self.inner.apply(batch).await
        }
        fn note_mutation(&self, mutation: impl FnOnce() -> Mutation) {
            self.noted.borrow_mut().push(mutation());
        }
    }

    // Hand-rolled NonUnique fixture (core can't use the proc-macro).
    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Note {
        text: String,
    }
    impl WaveDbStruct for Note {
        const STRUCT_HASH: u64 = 0x1107_0001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }
    impl NonUniqueStruct for Note {
        type Pivot = NotePivot;
        const NUM_SECONDARIES: usize = 0;
        fn secondary_key(&self, _: usize) -> Vec<u8> {
            Vec::new()
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct NotePivot {
        recency: ChainRoots,
        removals: LogRoots,
        permission: Option<PermissionRef>,
    }
    impl Pivot for NotePivot {
        const STRUCT_HASH: u64 = 0x1107_0002;
        fn secondaries(&self) -> &[ChainRoots] {
            &[]
        }
        fn recency(&self) -> ChainRoots {
            self.recency
        }
        fn removals(&self) -> LogRoots {
            self.removals
        }
        fn permission(&self) -> Option<&PermissionRef> {
            self.permission.as_ref()
        }
        fn replace_roots(&self, roots: Roots<'_>) -> Self {
            Self {
                recency: roots.recency,
                removals: roots.removals,
                permission: self.permission.clone(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Profile {
        city: String,
    }
    impl WaveDbStruct for Profile {
        const STRUCT_HASH: u64 = 0x1107_0003;
        const SHAPE: Shape = Shape::Unique;
        type PivotId = ();
    }

    fn note(text: &str) -> Note {
        Note { text: text.into() }
    }

    #[test]
    fn each_collection_op_notes_exactly_one_mutation() {
        block_on(async {
            let tenant = U48::from(6u32);
            let store = Recording::default();
            let pivot =
                Collection::<Note>::create(&store, tenant).await.unwrap();
            assert!(
                store.noted.borrow().is_empty(),
                "pivot creation is addressing, not a record mutation"
            );
            let col = Collection::<Note>::at(pivot, tenant);

            let id = col.insert(&store, &note("hi")).await.unwrap();
            col.save(&store, id, &note("edited")).await.unwrap();
            assert!(col.remove(&store, id).await.unwrap());
            // A re-remove mutates nothing, so it must note nothing.
            assert!(!col.remove(&store, id).await.unwrap());

            let noted = store.noted.borrow();
            assert_eq!(noted.len(), 3);
            for m in noted.iter() {
                assert_eq!(m.struct_hash, Note::STRUCT_HASH);
                assert_eq!(m.tenant, tenant);
                assert_eq!(m.pivot, Some(pivot));
                assert_eq!(m.id, id);
            }
            assert_eq!(noted[0].kind, MutationKind::Saved);
            assert_eq!(noted[0].body, to_wire(&note("hi")));
            assert_eq!(noted[1].kind, MutationKind::Saved);
            assert_eq!(noted[1].body, to_wire(&note("edited")));
            let MutationKind::Removed(removed_at) = noted[2].kind else {
                panic!("a remove notes a removal");
            };
            let saved_at =
                noted[1].meta.as_ref().map_or(0, |m| m.succession.instant());
            assert!(
                removed_at > saved_at,
                "the removal instant lands past every earlier mutation — \
                 a catch-up cursor advanced to it misses nothing"
            );
            assert!(noted[2].body.is_empty(), "{:?}", noted[2].body);
            // Saved events carry the new live metadata; a removal rewrites
            // no record, so it carries none.
            assert!(noted[0].meta.is_some() && noted[1].meta.is_some());
            assert!(noted[2].meta.is_none());
        });
    }

    #[test]
    fn unique_save_notes_the_anchor_with_no_pivot() {
        block_on(async {
            let tenant = U48::from(7u32);
            let store = Recording::default();
            let profile = Profile {
                city: "Lisbon".into(),
            };
            save_unique(&store, tenant, &profile).await.unwrap();

            let noted = store.noted.borrow();
            assert_eq!(noted.len(), 1);
            assert_eq!(noted[0].struct_hash, Profile::STRUCT_HASH);
            assert_eq!(noted[0].pivot, None);
            assert_eq!(
                noted[0].id,
                Id::new(Profile::STRUCT_HASH, tenant, true, 0),
                "the event names the fixed anchor"
            );
            assert_eq!(noted[0].kind, MutationKind::Saved);
            assert_eq!(noted[0].body, to_wire(&profile));
        });
    }

    #[test]
    fn a_failed_apply_notes_nothing() {
        block_on(async {
            // A store that refuses every batch: the note must never fire —
            // events only describe *committed* mutations.
            #[derive(Default)]
            struct Refusing(RefCell<Vec<Mutation>>);
            impl Store for Refusing {
                fn get(
                    &self,
                    _: Id,
                ) -> impl Future<Output = Result<Option<Vec<u8>>>>
                {
                    std::future::ready(Ok(None))
                }
                fn apply(
                    &self,
                    _: &[Write],
                ) -> impl Future<Output = Result<()>> {
                    std::future::ready(Err(crate::error::Error::Backend(
                        "full".into(),
                    )))
                }
                fn note_mutation(&self, mutation: impl FnOnce() -> Mutation) {
                    self.0.borrow_mut().push(mutation());
                }
            }
            let tenant = U48::from(8u32);
            let store = Refusing::default();
            let profile = Profile {
                city: "Oslo".into(),
            };
            assert!(save_unique(&store, tenant, &profile).await.is_err());
            assert!(store.0.borrow().is_empty());
        });
    }
}
