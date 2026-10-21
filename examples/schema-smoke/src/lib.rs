//! M1 smoke: what the `#[wavedb]` derive alone guarantees, proven end-to-end
//! without any node, transport, or `Db` — `STRUCT_HASH` identity, `WaveWire`
//! round-trips, shape consts, the generated NonUnique collection types, and
//! the **exposure declarations** (`expose_server!` / `expose_client!`): the
//! lists ARE the registry, and only listed items are dispatchable.
//!
//! # The exposure collision guard
//!
//! Both halves of the guard resolve while the schema compiles — these doctests
//! are the proof, since neither outcome is observable from a test body.
//!
//! Two entries that hash to the same 64-bit `STRUCT_HASH` are **one identity**
//! on the wire; the guard refuses the build:
//!
//! ```compile_fail
//! use wavedb_macros::{expose_server, wavedb};
//!
//! // Same name, same shape, same fields ⇒ same STRUCT_HASH, whatever the path.
//! mod first {
//!     #[wavedb_macros::wavedb]
//!     pub struct Twin { pub n: u64 }
//! }
//! mod second {
//!     #[wavedb_macros::wavedb]
//!     pub struct Twin { pub n: u64 }
//! }
//!
//! expose_server! { first::Twin, second::Twin }
//! ```
//!
//! Sharing only the low 15 bits (`type_salt`) still reads correctly, so it is a
//! **warning**, not an error — here promoted to one by `deny`:
//!
//! ```compile_fail
//! #![deny(deprecated)]
//! use wavedb_macros::{expose_server, wavedb};
//!
//! // A searched-for pair: distinct hashes, identical low 15 bits.
//! #[wavedb]
//! pub struct Probe184 { pub n: u64 }
//! #[wavedb]
//! pub struct Probe248 { pub n: u64 }
//!
//! expose_server! { Probe184, Probe248 }
//! ```
//!
//! A registry with no clash stays silent under the same `deny`:
//!
//! ```
//! #![deny(deprecated)]
//! use wavedb_macros::{expose_server, wavedb};
//!
//! #[wavedb]
//! pub struct Probe184 { pub n: u64 }
//! #[wavedb]
//! pub struct Probe185 { pub n: u64 }
//!
//! expose_server! { Probe184, Probe185 }
//! ```

use wavedb_macros::{expose_client, expose_server, wavedb};

// ── Exposure: what each side actually serves / can call ──────────────────────
//
// Entries are plain Rust paths (submodule items work — no scanner). `Invoice`
// hardens its surface: `save` swaps to an audited reimplementation inside the
// match arm at expansion time, `get` is excluded — a `get` command for it
// fails as an unknown hash, indistinguishable from a type that never existed.
expose_server! {
    AboutUser,
    Note,
    Row,
    billing::Invoice { save: audited_invoice_save, get: never },
    store Attachment,
}

expose_client! { AboutUser, Note, Row }

/// A hardened per-op override — same signature as the generated step; the
/// exposure arm calls this path instead (compiler-resolved, no callback).
/// Referenced only by `expose_server!`'s arms, so it rides `server-side`
/// like them — the pattern for any hand-written server-only item.
// Store-generic seam — Send only when the backing store is (workspace stance).
#[cfg(feature = "server-side")]
#[allow(clippy::future_not_send)]
async fn audited_invoice_save<S: wavedb_core::Store>(
    store: &S,
    caller: wavedb_core::Caller,
    payload: &[u8],
) -> wavedb_core::Result<wavedb_core::expose::Reply> {
    AUDITED_SAVES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    billing::Invoice::__wavedb_save(store, caller, payload).await
}

/// How many saves went through the audit override (test observability).
#[cfg(feature = "server-side")]
static AUDITED_SAVES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

/// Unique: one live record per tenant.
#[wavedb]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct AboutUser {
    pub name: String,
    pub city: String,
}

/// NonUnique: many per tenant, with a secondary index on `pinned`.
#[wavedb(NonUnique)]
#[wavedb::pivot(pinned)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Note {
    pub body: String,
    pub pinned: bool,
}

/// NonUnique with a **natural key**: the anchor is derived from `slug`
/// (SeaHash over its wire bytes), so `insert` is an upsert — one key
/// value, one record, in any process.
///
/// The declaration folds into the STRUCT_HASH: changing the key is a
/// schema change like any other.
#[wavedb(NonUnique)]
#[wavedb::key(slug)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Setting {
    pub slug: String,
    pub value: String,
}

/// NonUnique with a declared **segment capacity**: its declared list holds
/// 4…8 records per segment instead of the default 16…32 (RFC 0052).
///
/// `page` is the pagination unit — declare the page size the UI renders and a
/// rendered page becomes one segment read. It governs the **lists**, which are
/// the chains that hold records; the built-in chain holds only pointers and
/// takes a large capacity of its own (RFC 0054). It folds into the STRUCT_HASH,
/// so a chain is only ever laid out at one capacity.
#[wavedb(NonUnique, page = 4)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Row {
    #[wavedb::list]
    pub n: u64,
}

/// [`Row`]'s twin at the default capacity — same name, same field, same shape,
/// same list. Its only purpose is to prove `page` reaches the identity.
pub mod default_paged {
    use wavedb_macros::wavedb;

    #[wavedb(NonUnique)]
    #[derive(Debug, PartialEq, Eq, Clone, Default)]
    pub struct Row {
        #[wavedb::list]
        pub n: u64,
    }
}

/// NonUnique with two **declared lists** (RFC 0051): a second and third chain
/// of the same records, kept sorted by `name` and by `(city, name)` instead of
/// by modification instant.
///
/// The field spelling marks the field that *is* the ordering; the struct
/// spelling names a composite. Both fold into the STRUCT_HASH — a list is a
/// materialised copy of every record, so declaring one is a schema change.
///
/// The composite carries its **own** `page`: the built-in chain is rewritten at
/// its growth end on every save and so wants a small N, while a list is
/// rewritten in place and can hold the page a view renders (RFC 0052).
#[wavedb(NonUnique, page = 4)]
#[wavedb::list((city, name), page = 16)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Person {
    #[wavedb::list]
    pub name: String,
    pub city: String,
}

/// [`Person`]'s twin declaring **no** list — same name, same fields, same
/// shape, same `page`. Its only purpose is to prove a list reaches the identity.
pub mod unlisted {
    use wavedb_macros::wavedb;

    #[wavedb(NonUnique, page = 4)]
    #[derive(Debug, PartialEq, Eq, Clone, Default)]
    pub struct Person {
        pub name: String,
        pub city: String,
    }
}

/// [`Person`]'s twin differing **only** in the composite list's capacity — same
/// name, same fields, same shape, same struct `page`, same two orderings.
///
/// It proves the per-list capacity folds. It has to: a chain laid out at 16 and
/// one laid out at 32 have different split and merge triggers, so sharing an
/// identity would let one chain hold segments from both regimes and quietly
/// falsify the "a rendered page is one segment read" guarantee (RFC 0052).
pub mod wide_list {
    use wavedb_macros::wavedb;

    #[wavedb(NonUnique, page = 4)]
    #[wavedb::list((city, name), page = 32)]
    #[derive(Debug, PartialEq, Eq, Clone, Default)]
    pub struct Person {
        #[wavedb::list]
        pub name: String,
        pub city: String,
    }
}

/// NonUnique with a **fuzzy index** (RFC 0056): an n-gram posting tree over
/// `name`, so `"jhon smtih"` still finds `"John Smith"`.
///
/// The declaration sits on the **field**, not the struct header — a fuzzy
/// index is built over exactly one string, so a header form would only restate
/// a name the attribute already sits next to. It coexists with a
/// `#[wavedb::pivot]` on the same field on purpose: exact lookup and
/// approximate lookup are different questions, and neither is sugar for the
/// other.
#[wavedb(NonUnique)]
#[wavedb::pivot(city)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Member {
    #[wavedb::fuzzy]
    pub name: String,
    pub city: String,
}

/// [`Member`]'s twin at a **different gram width**.
///
/// Its only purpose is to prove `n` reaches the identity: postings cut at 3 and
/// at 4 are different keys entirely, so sharing a hash would let one tree hold
/// both and answer neither correctly.
pub mod wide_gram {
    use wavedb_macros::wavedb;

    #[wavedb(NonUnique)]
    #[wavedb::pivot(city)]
    #[derive(Debug, PartialEq, Eq, Clone, Default)]
    pub struct Member {
        #[wavedb::fuzzy(n = 4)]
        pub name: String,
        pub city: String,
    }
}

/// [`Member`]'s twin differing only in the **fold profile**.
///
/// `fold = none` keeps diacritics, so `José` and `Jose` are filed under
/// different grams — which is exactly why the profile has to reach the
/// identity too.
pub mod unfolded {
    use wavedb_macros::wavedb;

    #[wavedb(NonUnique)]
    #[wavedb::pivot(city)]
    #[derive(Debug, PartialEq, Eq, Clone, Default)]
    pub struct Member {
        #[wavedb::fuzzy(fold = none)]
        pub name: String,
        pub city: String,
    }
}

/// A struct in a submodule — items are named by path, not found by a scanner.
pub mod billing {
    use wavedb_macros::wavedb;

    #[wavedb]
    #[derive(Debug, PartialEq, Eq, Clone, Default)]
    pub struct Invoice {
        pub cents: u64,
    }
}

/// Already-compressed payloads opt their pages out of zstd. Storage policy —
/// but it reaches stored bytes, so it **folds into the hash** (RFC 0052):
/// flipping it is a new type, not a live setting.
#[wavedb(compress = false)]
#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct Attachment {
    pub media: Vec<u8>,
}

/// A twin of [`Attachment`], differing **only** in the compression declaration.
///
/// Same struct name, same field, same shape — it lives in its own module so the
/// names really do collide, which is what makes the hash comparison in
/// `compression_folds_into_the_identity` meaningful: anything else about the two
/// would move the hash on its own.
pub mod compressed_twin {
    use wavedb_macros::wavedb;

    #[wavedb]
    #[derive(Debug, PartialEq, Eq, Clone, Default)]
    pub struct Attachment {
        pub media: Vec<u8>,
    }
}

#[cfg(test)]
mod tests {
    use super::billing::Invoice;
    use super::{AboutUser, Note, NotePivot, NotePivotId};
    use wavedb_core::traits::Shape;
    use wavedb_core::wire::{from_wire, to_wire};
    use wavedb_core::{LocalId, WaveDbStruct};

    /// The **generated** collection path produces `Send` futures over a
    /// `Send + Sync` store — the end-to-end version of the assertions in
    /// `wavedb-core` (the index layer) and `wavedb-storage` (the engine).
    ///
    /// This is the one that covers `#[wavedb]`'s expansion, so it is the one
    /// that would catch a macro change introducing an `Rc` or a `RefCell` into
    /// a generated body. Nothing else would: the expansion compiles fine
    /// non-`Send`, and the whole stack is driven single-threaded today, so the
    /// loss would surface only when
    /// [RFC 0064](../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md)'s
    /// multi-thread build was attempted — by which point the cause would be
    /// somewhere in a year of macro commits.
    ///
    /// `Person` is the widest declared type here (two lists, one composite),
    /// so it exercises the list-maintenance paths as well as the plain ones.
    const _: fn(&mem::MemStore) = |store| {
        fn assert_send<T: Send>(_: T) {}
        assert_send(async move {
            let db = wavedb_core::LocalHandle::new(
                store,
                wavedb_core::U48::from(7u32),
            );
            let col = super::Person::collection(
                super::Person::create_pivot(&db).await?,
            );
            col.insert(&db, &super::Person::default()).await?;
            col.get(&db, wavedb_core::Id::from_raw(0)).await
        });
    };

    // Every declared struct round-trips through its derive-emitted WaveWire
    // impl, and its STRUCT_HASH is a distinct compile-time const.
    #[test]
    fn derived_structs_roundtrip_and_hashes_differ() {
        let about = AboutUser {
            name: "Ada".into(),
            city: "London".into(),
        };
        let note = Note {
            body: "hi".into(),
            pinned: true,
        };
        let invoice = Invoice { cents: 42 };

        assert_eq!(from_wire::<AboutUser>(&to_wire(&about)), Ok(about));
        assert_eq!(from_wire::<Note>(&to_wire(&note)), Ok(note));
        assert_eq!(from_wire::<Invoice>(&to_wire(&invoice)), Ok(invoice));

        assert_ne!(AboutUser::STRUCT_HASH, Note::STRUCT_HASH);
        assert_ne!(AboutUser::STRUCT_HASH, Invoice::STRUCT_HASH);
        assert_ne!(Note::STRUCT_HASH, Invoice::STRUCT_HASH);
    }

    // Every declaration that reaches stored bytes folds into the identity
    // (RFC 0052), and `compress` is one: it decides whether a type's pages go
    // through zstd. WaveDB does no engine-side migration, so the only coherent
    // answer to "can I flip this on live data?" is that flipping it gives you a
    // **different type** and the move is application code.
    #[test]
    fn compression_folds_into_the_identity() {
        use crate::Attachment as Uncompressed;
        use crate::compressed_twin::Attachment as Compressed;

        // The two differ in nothing a hash sees except the declaration: same
        // struct name, same field name and type, same (default) shape.
        assert!(!Uncompressed::struct_storage().compress());
        assert!(Compressed::struct_storage().compress());
        assert_ne!(
            Uncompressed::STRUCT_HASH,
            Compressed::STRUCT_HASH,
            "flipping `compress` must mint a new type, not reinterpret the old \
             one's pages"
        );
        // And the lanes move with it, since they derive from the type's hash —
        // so the new type cannot land in the old one's storage.
        assert_ne!(
            Uncompressed::storage_entries()[0].struct_hash(),
            Compressed::storage_entries()[0].struct_hash(),
        );
    }

    // `page = N` has to do two things, and they fail independently: reach the
    // **identity** (so a chain is only ever laid out at one capacity) and reach
    // the **chain** (so the declaration actually changes the layout).
    #[test]
    fn a_declared_page_reaches_both_the_identity_and_the_layout() {
        use crate::Row as Paged;
        use crate::default_paged::Row as Default;
        use futures::executor::block_on;
        use wavedb_core::{NonUniqueStruct, U48};

        assert_eq!(Paged::PAGE, 4);
        assert_eq!(Default::PAGE, wavedb_core::index::DEFAULT_SEGMENT_MIN);
        assert_ne!(
            Paged::STRUCT_HASH,
            Default::STRUCT_HASH,
            "a different capacity must be a different type — otherwise one \
             chain could hold segments laid out two ways"
        );

        // And the layout: 20 records at `page = 4` must land in segments of
        // 4…8, never the default 16…32. Counting distinct segment ids is the
        // observation, since the collection API deliberately hides them.
        block_on(async {
            let store = mem::MemStore::default();
            let db = wavedb_core::LocalHandle::new(&store, U48::from(7u32));
            let paged =
                Paged::collection(Paged::create_pivot(&db).await.unwrap());
            let plain =
                Default::collection(Default::create_pivot(&db).await.unwrap());
            for n in 0..20u64 {
                paged.insert(&db, &Paged { n }).await.unwrap();
                plain.insert(&db, &Default { n }).await.unwrap();
            }
            // `LANE_HASHES[0]` is the record lane, and since RFC 0054 the only
            // things in it are the declared lists' segments — recency lives in
            // its own lane, being ids rather than records. So this counts the
            // list directly: at `page = 4`, 20 records need 4…8 per segment, so
            // at least 3; at the default 16…32 they fit in one.
            assert!(
                store.lane_values(Paged::LANE_HASHES[0]) >= 3,
                "the declared capacity did not reach the list's chain"
            );
            assert_eq!(
                store.lane_values(Default::LANE_HASHES[0]),
                1,
                "the default capacity fits 20 records in one list segment"
            );
        });
    }

    // A declared list is a materialised copy of every record, so — like every
    // other fact that reaches stored bytes — it folds into the identity.
    #[test]
    fn a_declared_list_folds_into_the_identity() {
        use super::Person as Listed;
        use super::unlisted::Person as Plain;
        use wavedb_core::NonUniqueStruct;

        assert_eq!(Listed::NUM_LISTS, 2, "one field-level, one struct-level");
        assert_eq!(Plain::NUM_LISTS, 0);
        assert_ne!(
            Listed::STRUCT_HASH,
            Plain::STRUCT_HASH,
            "declaring a list must be a new type — the engine does no \
             migration, so there is nowhere for the extra chain to come from"
        );
    }

    // A list's `page` is its own, not the struct's. Here: that the macro plumbs
    // it to the trait and folds it into the identity. That it reaches the
    // *layout* is pinned in `wavedb-core`, where the two chains can be measured
    // one at a time (`a_list_lays_out_at_its_own_page`).
    #[test]
    fn a_declared_list_page_is_independent_of_the_struct() {
        use super::Person;
        use wavedb_core::NonUniqueStruct;

        assert_eq!(Person::PAGE, 4);
        assert_eq!(Person::list_page(0), 4, "an undeclared list inherits");
        assert_eq!(Person::list_page(1), 16, "a declared list overrides");
        // Out of range falls back rather than panicking — nothing dispatches
        // there (the engine loops `0..NUM_LISTS`), but a const that traps would
        // be a footgun for any future caller.
        assert_eq!(Person::list_page(99), 4);

        // And it folds: the twin differs in nothing but this number.
        assert_eq!(super::wide_list::Person::list_page(1), 32);
        assert_ne!(
            Person::STRUCT_HASH,
            super::wide_list::Person::STRUCT_HASH,
            "a list's capacity must be a different type — one chain holding \
             segments laid out two ways is exactly what the fold prevents"
        );
    }

    // `#[wavedb::fuzzy]` sits on the **field**, and both of its knobs reach
    // stored bytes — the gram width decides what the keys *are*, the fold
    // decides which records share them. Neither can be changed on live data,
    // so both must mint a new type.
    #[test]
    fn a_fuzzy_declaration_and_its_profile_reach_the_identity() {
        use super::{Member, unfolded, wide_gram};
        use wavedb_core::NonUniqueStruct;
        use wavedb_core::fuzzy::{DEFAULT_N, Fold};

        assert_eq!(Member::NUM_FUZZY, 1);
        assert_eq!(Member::fuzzy_profile(0), (DEFAULT_N, Fold::Latin));
        assert_eq!(wide_gram::Member::fuzzy_profile(0), (4, Fold::Latin));
        assert_eq!(unfolded::Member::fuzzy_profile(0), (DEFAULT_N, Fold::None));

        // The source is the marked field, borrowed — no clone to index.
        let m = Member {
            name: "Ada".into(),
            city: "London".into(),
        };
        assert_eq!(m.fuzzy_source(0), "Ada");
        assert_eq!(m.fuzzy_source(9), "", "out of range falls back");

        // Three types, three identities: same name, same fields, same shape.
        assert_ne!(
            Member::STRUCT_HASH,
            wide_gram::Member::STRUCT_HASH,
            "grams cut at 3 and at 4 are different keys — one tree holding \
             both would answer neither correctly"
        );
        assert_ne!(
            Member::STRUCT_HASH,
            unfolded::Member::STRUCT_HASH,
            "`fold = none` files José and Jose apart; reusing the identity \
             would leave the old postings claiming the new rule"
        );
        assert_ne!(
            wide_gram::Member::STRUCT_HASH,
            unfolded::Member::STRUCT_HASH
        );
    }

    // The macro mirrors the engine's default gram width so it can fold a
    // **resolved** value rather than eliding it. If the two ever drift, an
    // undeclared `#[wavedb::fuzzy]` would hash as one width and be laid out at
    // another — silently.
    #[test]
    fn the_macro_default_matches_the_engine() {
        use super::Member;
        use wavedb_core::NonUniqueStruct;

        assert_eq!(
            Member::fuzzy_profile(0).0,
            wavedb_core::fuzzy::DEFAULT_N,
            "the macro's mirrored DEFAULT_N drifted from the engine's"
        );
    }

    // The generated `listed_by_*` readers walk their own chain, in their own
    // order — not the built-in chain's recency order.
    #[test]
    fn a_declared_list_reads_in_its_own_order() {
        use super::{Person, PersonLists};
        use futures::TryStreamExt;
        use futures::executor::block_on;
        use wavedb_core::U48;

        block_on(async {
            let store = mem::MemStore::default();
            let db = wavedb_core::LocalHandle::new(&store, U48::from(7u32));
            let people =
                Person::collection(Person::create_pivot(&db).await.unwrap());
            // Inserted in an order that agrees with neither declared list.
            for (name, city) in
                [("carol", "lisboa"), ("alice", "porto"), ("bob", "lisboa")]
            {
                people
                    .insert(
                        &db,
                        &Person {
                            name: name.into(),
                            city: city.into(),
                        },
                    )
                    .await
                    .unwrap();
            }

            let by_name: Vec<String> = people
                .listed_by_name(&db)
                .map_ok(|p| p.name)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(by_name, ["alice", "bob", "carol"]);

            // The composite orders by city first, so both lisboa rows precede
            // the porto one however their names sort.
            let by_city: Vec<String> = people
                .listed_by_city_name(&db)
                .map_ok(|p| format!("{}/{}", p.city, p.name))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(by_city, ["lisboa/bob", "lisboa/carol", "porto/alice"]);

            // The built-in chain is untouched by any of this: it still reads
            // most-recently-written first.
            let recent: Vec<String> = people
                .all(&db)
                .map_ok(|p| p.name)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(recent, ["bob", "alice", "carol"]);

            assert_eq!(people.list_len(&db, 0).await.unwrap(), 3);
        });
    }

    // A list holds only living records, and it re-sorts on a save: both are
    // maintained inside the one atomic batch the mutation already writes.
    #[test]
    fn a_list_tracks_saves_and_removals() {
        use super::{Person, PersonLists};
        use futures::TryStreamExt;
        use futures::executor::block_on;
        use wavedb_core::U48;

        block_on(async {
            let store = mem::MemStore::default();
            let db = wavedb_core::LocalHandle::new(&store, U48::from(7u32));
            let people =
                Person::collection(Person::create_pivot(&db).await.unwrap());
            let mut ids = Vec::new();
            for name in ["alice", "bob", "carol"] {
                ids.push(
                    people
                        .insert(
                            &db,
                            &Person {
                                name: name.into(),
                                city: "porto".into(),
                            },
                        )
                        .await
                        .unwrap(),
                );
            }

            // Renaming alice → zoe must move her to the far end of the list.
            people
                .save(
                    &db,
                    ids[0],
                    &Person {
                        name: "zoe".into(),
                        city: "porto".into(),
                    },
                )
                .await
                .unwrap();
            let names: Vec<String> = people
                .listed_by_name(&db)
                .map_ok(|p| p.name)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(names, ["bob", "carol", "zoe"]);

            assert!(people.remove(&db, ids[1]).await.unwrap());
            let names: Vec<String> = people
                .listed_by_name(&db)
                .map_ok(|p| p.name)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(names, ["carol", "zoe"], "a removal leaves every list");
            assert_eq!(people.list_len(&db, 0).await.unwrap(), 2);
        });
    }

    // The pager: `_at_page` descends the sparse index to a page boundary
    // rather than walking to it, and `page = 4` makes a page one segment.
    #[test]
    fn a_list_pages_by_descent() {
        use super::{Person, PersonLists};
        use futures::executor::block_on;
        use futures::{StreamExt, TryStreamExt};
        use wavedb_core::U48;

        block_on(async {
            let store = mem::MemStore::default();
            let db = wavedb_core::LocalHandle::new(&store, U48::from(7u32));
            let people =
                Person::collection(Person::create_pivot(&db).await.unwrap());
            for n in 0..12u32 {
                people
                    .insert(
                        &db,
                        &Person {
                            // Zero-padded so the byte order is the number order.
                            name: format!("p{n:02}"),
                            city: "porto".into(),
                        },
                    )
                    .await
                    .unwrap();
            }
            assert_eq!(people.list_len(&db, 0).await.unwrap(), 12);

            let page: Vec<String> = people
                .listed_by_name_at_page(&db, 2, 4)
                .map_ok(|p| p.name)
                .take(4)
                .try_collect()
                .await
                .unwrap();
            assert_eq!(page, ["p08", "p09", "p10", "p11"]);

            // Past the end yields nothing rather than failing.
            let none: Vec<String> = people
                .listed_by_name_at_page(&db, 99, 4)
                .map_ok(|p| p.name)
                .try_collect()
                .await
                .unwrap();
            assert!(none.is_empty(), "{none:?}");
        });
    }

    // Shape is a compile-time `const` on the type — no runtime lookup.
    #[test]
    fn shape_is_a_const_not_a_lookup() {
        assert_eq!(AboutUser::SHAPE, Shape::Unique);
        assert_eq!(Note::SHAPE, Shape::NonUnique);
        assert_eq!(Invoice::SHAPE, Shape::Unique);
    }

    // The NonUnique derive emits the collection machinery: a typed PivotId
    // handle and a Pivot with the record chain's and removal log's roots,
    // plus one secondary slot per `#[wavedb::pivot(...)]`.
    #[test]
    fn nonunique_generates_pivot_types() {
        let pivot = NotePivot {
            recency: wavedb_core::ChainRoots {
                head: LocalId::new(10, false, 1),
                tail: LocalId::new(11, false, 1),
                index: LocalId::new(12, false, 1),
            },
            removals: wavedb_core::LogRoots {
                head: LocalId::new(20, false, 2),
                tail: LocalId::new(21, false, 2),
            },
            ..NotePivot::default()
        };
        assert_eq!(pivot.secondaries.len(), 1, "one #[wavedb::pivot(...)]");
        assert_eq!(from_wire::<NotePivot>(&to_wire(&pivot)), Ok(pivot));

        // The typed handle is what a holder stores to reference the collection.
        let handle: <Note as WaveDbStruct>::PivotId =
            NotePivotId::new(LocalId::new(7, false, 0));
        assert_eq!(handle.local_id(), LocalId::new(7, false, 0));
    }

    /// A minimal in-memory `Store` — the whole backend contract the derived
    /// API needs (`get` + atomic `apply`).
    mod mem {
        use std::collections::BTreeMap;
        use std::sync::Mutex;
        use wavedb_core::{Id, Result, Store, Write};

        #[derive(Default)]
        pub struct MemStore(Mutex<BTreeMap<u128, Vec<u8>>>);

        impl MemStore {
            /// How many stored values carry `lane_hash` at their head.
            ///
            /// Every WaveDB value is `[STRUCT_HASH LE][…]`, and a chain
            /// segment's head is its **lane** hash — so this counts the
            /// segments of one lane without an accessor the engine does not
            /// otherwise need.
            pub fn lane_values(&self, lane_hash: u64) -> usize {
                self.0
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|bytes| {
                        bytes.get(..8).and_then(|h| h.try_into().ok())
                            == Some(lane_hash.to_le_bytes())
                    })
                    .count()
            }
        }

        // A test double, written in the trait's own (async) shape.
        #[allow(clippy::unused_async_trait_impl)]
        impl Store for MemStore {
            async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
                Ok(self.0.lock().unwrap().get(&id.raw()).cloned())
            }
            async fn apply(&self, batch: &[Write]) -> Result<()> {
                let mut m = self.0.lock().unwrap();
                for w in batch {
                    if let Write::Expect(_, id, expected) = w
                        && m.get(&id.raw()) != expected.as_ref()
                    {
                        return Err(wavedb_core::Error::Conflict(*id));
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
    }

    // Native targets get compile-time storage declarations: each type carries
    // its own `StructStorage` static (hash + compression policy, no state)
    // plus the `storage_entries()` registry list — no runtime STRUCT_HASH map.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn native_storage_statics_are_per_type() {
        // One slot per type, stamped with that type's own hash.
        assert_eq!(
            AboutUser::struct_storage().struct_hash(),
            AboutUser::STRUCT_HASH
        );
        assert_eq!(Note::struct_storage().struct_hash(), Note::STRUCT_HASH);
        assert!(!std::ptr::eq(
            AboutUser::struct_storage(),
            Note::struct_storage()
        ));

        // Compression is per-type policy: on by default, opted out at the
        // declaration (`#[wavedb(compress = false)]`).
        assert!(Note::struct_storage().compress());
        assert!(!crate::Attachment::struct_storage().compress());

        // Unique registers itself; NonUnique bundles its Pivot's slot and the
        // four reserved lanes its collection lives in — declared-list segments,
        // the recency chain, the removal log, and the sparse index. Each is its
        // own directory so a page never mixes fat records with skinny id
        // entries, and so each lane's zstd dictionary models one kind of thing.
        assert_eq!(AboutUser::storage_entries().len(), 1);
        let entries = Note::storage_entries();
        assert_eq!(entries.len(), 6);
        assert!(std::ptr::eq(entries[0], Note::struct_storage()));
        assert!(std::ptr::eq(entries[1], NotePivot::struct_storage()));
        assert!(std::ptr::eq(entries[2], Note::records_lane_storage()));
        assert!(std::ptr::eq(entries[3], Note::recency_lane_storage()));
        assert!(std::ptr::eq(entries[4], Note::dead_lane_storage()));
        assert!(std::ptr::eq(entries[5], Note::index_lane_storage()));

        // The exposure's StorageRegistry flattens struct entries AND the
        // `store` entry — Attachment's slot registers with no wire surface.
        let slots =
            wavedb_storage::StorageRegistry::storage_entries(&super::REGISTRY);
        assert!(
            slots
                .iter()
                .any(|s| std::ptr::eq(*s, crate::Attachment::struct_storage())),
            "`store Attachment` must contribute its engine slot"
        );
    }

    /// `true` when a dispatch refused as an unknown hash.
    fn unknown(r: &wavedb_core::Result<wavedb_core::expose::Reply>) -> bool {
        matches!(r, Err(wavedb_core::Error::UnknownStructHash(_)))
    }

    // Exposure reachability + refusals: the lists are the registry; an
    // unlisted type, a wrong-shape command, and an excluded op all refuse
    // uniformly as an unknown hash.
    #[test]
    fn exposure_reachability_and_uniform_refusals() {
        use futures::executor::block_on;
        use wavedb_core::U48;
        use wavedb_core::expose::{Command, Exposure as _};
        use wavedb_core::wire::to_wire;

        use super::REGISTRY;

        block_on(async {
            let store = mem::MemStore::default();
            let tenant = U48::from(9u32);
            let caller = wavedb_core::Caller::tenant_owned(tenant);

            // Reachability is exactly the list.
            assert!(REGISTRY.knows(AboutUser::STRUCT_HASH));
            assert!(REGISTRY.knows(Invoice::STRUCT_HASH));
            assert!(!REGISTRY.knows(0xDEAD_BEEF));
            // A `store` entry has NO wire surface: its hash refuses exactly
            // like a type that never existed, while its engine slots ride
            // the StorageRegistry (asserted in the native storage test).
            assert!(
                !REGISTRY.knows(super::Attachment::STRUCT_HASH),
                "storage-only"
            );
            assert!(unknown(
                &REGISTRY
                    .execute(
                        &store,
                        caller,
                        super::Attachment::STRUCT_HASH,
                        Command::Get,
                        &[],
                    )
                    .await
            ));

            // Wire gate: bodies must decode as the declared type.
            let ada = AboutUser {
                name: "Ada".into(),
                city: "London".into(),
            };
            assert!(
                REGISTRY
                    .decode_check(AboutUser::STRUCT_HASH, &to_wire(&ada))
                    .is_ok()
            );
            assert!(
                REGISTRY
                    .decode_check(AboutUser::STRUCT_HASH, &[1, 2, 3])
                    .is_err()
            );

            // Unlisted hash, wrong-shape command, and excluded op.
            assert!(unknown(
                &REGISTRY
                    .execute(&store, caller, 0xDEAD_BEEF, Command::Get, &[])
                    .await
            ));
            assert!(unknown(
                &REGISTRY
                    .execute(
                        &store,
                        caller,
                        AboutUser::STRUCT_HASH,
                        Command::Insert,
                        &[],
                    )
                    .await
            ));
            assert!(unknown(
                &REGISTRY
                    .execute(
                        &store,
                        caller,
                        Invoice::STRUCT_HASH,
                        Command::Get,
                        &[],
                    )
                    .await
            ));
        });
    }

    // Exposure execution: the Unique round-trip, the override arm, and the
    // client registry's engine-less surface.
    #[test]
    fn exposure_dispatch_end_to_end() {
        use futures::executor::block_on;
        use wavedb_core::U48;
        use wavedb_core::expose::{Command, Exposure as _, Reply};
        use wavedb_core::wire::to_wire;

        use super::{CLIENT_REGISTRY, REGISTRY};

        block_on(async {
            let store = mem::MemStore::default();
            let tenant = U48::from(9u32);
            let caller = wavedb_core::Caller::tenant_owned(tenant);

            // Unique: Save then Get round-trips through the dispatch.
            let ada = AboutUser {
                name: "Ada".into(),
                city: "London".into(),
            };
            let done = REGISTRY
                .execute(
                    &store,
                    caller,
                    AboutUser::STRUCT_HASH,
                    Command::Save,
                    &to_wire(&ada),
                )
                .await
                .unwrap();
            assert_eq!(done, Reply::Done);
            let got = REGISTRY
                .execute(
                    &store,
                    caller,
                    AboutUser::STRUCT_HASH,
                    Command::Get,
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(got, Reply::Value(Some(to_wire(&ada))));

            // The override path serves Invoice saves (audited, then stored).
            let before =
                super::AUDITED_SAVES.load(std::sync::atomic::Ordering::Relaxed);
            REGISTRY
                .execute(
                    &store,
                    caller,
                    Invoice::STRUCT_HASH,
                    Command::Save,
                    &to_wire(&Invoice { cents: 12 }),
                )
                .await
                .unwrap();
            assert_eq!(
                super::AUDITED_SAVES.load(std::sync::atomic::Ordering::Relaxed),
                before + 1,
                "the arm must route through the override"
            );

            // The client registry only gates reachability — it never executes.
            assert!(CLIENT_REGISTRY.knows(Note::STRUCT_HASH));
            assert!(!CLIENT_REGISTRY.knows(Invoice::STRUCT_HASH));
            assert!(unknown(
                &CLIENT_REGISTRY
                    .execute(
                        &store,
                        caller,
                        AboutUser::STRUCT_HASH,
                        Command::Get,
                        &[],
                    )
                    .await
            ));
        });
    }

    // A verified caller whose two identity halves disagree is refused before
    // the engine sees it. The engine isolates by tenant and stamps `user` as
    // provenance only, so `user != tenant` asks an intra-tenant authorization
    // question nothing answers yet — the honest reply is a typed refusal, not
    // a write that would silently grant the whole tenant.
    #[test]
    fn a_caller_whose_user_is_not_its_tenant_is_refused() {
        use futures::executor::block_on;
        use wavedb_core::U48;
        use wavedb_core::expose::{Command, Exposure as _};
        use wavedb_core::wire::to_wire;

        use super::REGISTRY;

        block_on(async {
            let store = mem::MemStore::default();
            let tenant = U48::from(9u32);
            let stranger = wavedb_core::Caller {
                user: U48::from(10u32),
                tenant,
            };
            let ada = AboutUser {
                name: "Ada".into(),
                city: "London".into(),
            };

            let refused = REGISTRY
                .execute(
                    &store,
                    stranger,
                    AboutUser::STRUCT_HASH,
                    Command::Save,
                    &to_wire(&ada),
                )
                .await;
            assert!(
                matches!(
                    refused,
                    Err(wavedb_core::Error::IdentityMismatch(u, t))
                        if u == U48::from(10u32) && t == tenant
                ),
                "expected IdentityMismatch, got {refused:?}"
            );

            // Reads refuse on the same terms: the mismatch is about who the
            // caller is, not about which way the bytes travel.
            let refused = REGISTRY
                .execute(
                    &store,
                    stranger,
                    AboutUser::STRUCT_HASH,
                    Command::Get,
                    &[],
                )
                .await;
            assert!(
                matches!(
                    refused,
                    Err(wavedb_core::Error::IdentityMismatch(..))
                ),
                "expected IdentityMismatch, got {refused:?}"
            );

            // And nothing was written on the way to the refusal.
            let owner = wavedb_core::Caller::tenant_owned(tenant);
            let got = REGISTRY
                .execute(
                    &store,
                    owner,
                    AboutUser::STRUCT_HASH,
                    Command::Get,
                    &[],
                )
                .await
                .unwrap();
            assert_eq!(
                got,
                wavedb_core::expose::Reply::Value(None),
                "the refused save must not have reached the store"
            );
        });
    }

    // The NonUnique command set through the dispatch: Insert mints, Get
    // resolves, Update re-keys, Remove moves to dead — each naming the
    // collection's Pivot first.
    #[test]
    fn exposure_nonunique_commands_drive_the_collection() {
        use futures::TryStreamExt;
        use futures::executor::block_on;
        use wavedb_core::U48;
        use wavedb_core::expose::{Command, Exposure as _, Reply};
        use wavedb_core::wire::to_wire;

        use super::NoteSecondaries as _;
        use super::REGISTRY;

        block_on(async {
            let store = mem::MemStore::default();
            let tenant = U48::from(11u32);
            let caller = wavedb_core::Caller::tenant_owned(tenant);
            let db = wavedb_core::LocalHandle::new(&store, tenant);
            let pivot = Note::create_pivot(&db).await.unwrap();
            let col = Note::collection(pivot);

            // Insert via the wire shape: (pivot LocalId, body).
            let note = Note {
                body: "hi".into(),
                pinned: false,
            };
            let Reply::Inserted(id) = REGISTRY
                .execute(
                    &store,
                    caller,
                    Note::STRUCT_HASH,
                    Command::Insert,
                    &to_wire(&(pivot.local_id(), note.clone())),
                )
                .await
                .unwrap()
            else {
                panic!("insert must mint an id")
            };

            // Get by `(pivot, id)`.
            assert_eq!(
                REGISTRY
                    .execute(
                        &store,
                        caller,
                        Note::STRUCT_HASH,
                        Command::Get,
                        &to_wire(&(pivot.local_id(), id)),
                    )
                    .await
                    .unwrap(),
                Reply::Value(Some(to_wire(&note)))
            );

            // Update re-keys the `pinned` secondary index.
            let pinned_now = Note {
                body: "hi".into(),
                pinned: true,
            };
            REGISTRY
                .execute(
                    &store,
                    caller,
                    Note::STRUCT_HASH,
                    Command::Update,
                    &to_wire(&(pivot.local_id(), id, pinned_now)),
                )
                .await
                .unwrap();
            let pinned: Vec<Note> =
                col.by_pinned(&db, &true).try_collect().await.unwrap();
            assert_eq!(
                pinned.iter().map(|n| n.pinned).collect::<Vec<_>>(),
                vec![true],
                "the update must land in the secondary index"
            );

            // Remove moves it out of the living walk.
            assert_eq!(
                REGISTRY
                    .execute(
                        &store,
                        caller,
                        Note::STRUCT_HASH,
                        Command::Remove,
                        &to_wire(&(pivot.local_id(), id)),
                    )
                    .await
                    .unwrap(),
                Reply::Removed(true)
            );
            let live: Vec<Note> = col.all(&db).try_collect().await.unwrap();
            assert!(live.is_empty(), "{live:?}");
        });
    }

    // A command names its collection: through another Pivot a record is
    // absent, and it cannot be changed or removed.
    #[test]
    fn a_command_through_another_pivot_cannot_reach_the_record() {
        use futures::executor::block_on;
        use wavedb_core::expose::{Command, Exposure as _, Reply};
        use wavedb_core::wire::to_wire;
        use wavedb_core::{Error, LocalId, U48};

        use super::REGISTRY;

        block_on(async {
            let store = mem::MemStore::default();
            let tenant = U48::from(11u32);
            let caller = wavedb_core::Caller::tenant_owned(tenant);
            let db = wavedb_core::LocalHandle::new(&store, tenant);
            let pivot = Note::create_pivot(&db).await.unwrap();
            let note = Note {
                body: "hi".into(),
                pinned: false,
            };
            let id = Note::collection(pivot).insert(&db, &note).await.unwrap();
            let store = &store;
            let run = |command, payload: Vec<u8>| async move {
                REGISTRY
                    .execute(
                        store,
                        caller,
                        Note::STRUCT_HASH,
                        command,
                        &payload,
                    )
                    .await
            };

            let elsewhere = LocalId::new(1, false, 0);
            let get = run(Command::Get, to_wire(&(elsewhere, id))).await;
            assert_eq!(get.unwrap(), Reply::Value(None));
            let update =
                run(Command::Update, to_wire(&(elsewhere, id, note.clone())));
            assert!(
                matches!(update.await, Err(Error::RecordMissing(m)) if m == id)
            );
            let remove = run(Command::Remove, to_wire(&(elsewhere, id))).await;
            assert!(matches!(remove, Err(Error::RecordMissing(m)) if m == id));

            // Through its own Pivot it is all still there.
            let get = run(Command::Get, to_wire(&(pivot.local_id(), id))).await;
            assert_eq!(get.unwrap(), Reply::Value(Some(to_wire(&note))));
        });
    }

    // A route handle runs every typed operation through the registry's
    // `run_node` match — `store`-only types included, which the wire cannot
    // name — and gives back what the engine-local handle would.
    #[test]
    fn a_route_handle_runs_every_operation_through_the_registry() {
        use futures::TryStreamExt;
        use futures::executor::block_on;
        use wavedb_core::{LocalRoute, RouteHandle, U48};

        use super::{Attachment, REGISTRY};

        block_on(async {
            let store = mem::MemStore::default();
            let route = LocalRoute::new(&store, &REGISTRY);
            let db = RouteHandle::new(&route, U48::from(5u32));

            // Unique, `store`-only: unreachable over the wire, served here.
            let media = Attachment { media: vec![1, 2] };
            media.save(&db).await.unwrap();
            Attachment { media: vec![3] }.save(&db).await.unwrap();
            assert_eq!(
                Attachment::get(&db).await.unwrap(),
                Some(Attachment { media: vec![3] })
            );
            let versions: Vec<_> =
                Attachment::history(&db).try_collect().await.unwrap();
            assert_eq!(versions.len(), 2);

            // NonUnique: create (minted by the caller), insert, read, update,
            // walk, remove.
            let pivot = Note::create_pivot(&db).await.unwrap();
            let col = Note::collection(pivot);
            let note = Note {
                body: "a".into(),
                pinned: false,
            };
            let id = col.insert(&db, &note).await.unwrap();
            assert_eq!(col.get(&db, id).await.unwrap(), Some(note));
            let pinned = Note {
                body: "a".into(),
                pinned: true,
            };
            col.save(&db, id, &pinned).await.unwrap();
            let all: Vec<Note> = col.all(&db).try_collect().await.unwrap();
            assert_eq!(all, vec![pinned]);
            assert!(col.remove(&db, id).await.unwrap());
            let all: Vec<Note> = col.all(&db).try_collect().await.unwrap();
            assert!(all.is_empty(), "{all:?}");
        });
    }

    // The derived Unique surface keeps the timeline: `save` archives the
    // superseded version, the generated `history` walks it newest-first.
    #[test]
    fn derived_unique_history_walks_versions() {
        use futures::TryStreamExt;
        use futures::executor::block_on;
        use wavedb_core::U48;

        block_on(async {
            let store = mem::MemStore::default();
            let db = wavedb_core::LocalHandle::new(&store, U48::from(3u32));
            for city in ["Rome", "Oslo", "Lima"] {
                AboutUser {
                    name: "Ada".into(),
                    city: city.into(),
                }
                .save(&db)
                .await
                .unwrap();
            }
            let versions: Vec<(wavedb_core::Metadata, AboutUser)> =
                AboutUser::history(&db).try_collect().await.unwrap();
            assert_eq!(
                versions
                    .iter()
                    .map(|(_, u)| u.city.as_str())
                    .collect::<Vec<_>>(),
                vec!["Lima", "Oslo", "Rome"],
                "newest-first timeline"
            );
            assert_eq!(
                AboutUser::get(&db).await.unwrap().unwrap().city,
                "Lima"
            );
        });
    }

    // The generated secondary index end-to-end: `#[wavedb::pivot(pinned)]`
    // emits `by_pinned` on the collection handle; insert indexes, save
    // re-keys, remove de-indexes — all through the derived surface.
    #[test]
    fn derived_secondary_index_by_field() {
        use futures::TryStreamExt;
        use futures::executor::block_on;
        use wavedb_core::U48;

        use super::NoteSecondaries as _;

        block_on(async {
            let store = mem::MemStore::default();
            let db = wavedb_core::LocalHandle::new(&store, U48::from(7u32));
            let notes = Note::create_pivot(&db).await.unwrap();
            let col = Note::collection(notes);

            let a = col
                .insert(
                    &db,
                    &Note {
                        body: "keep".into(),
                        pinned: true,
                    },
                )
                .await
                .unwrap();
            let b = col
                .insert(
                    &db,
                    &Note {
                        body: "later".into(),
                        pinned: false,
                    },
                )
                .await
                .unwrap();

            let by_body = |notes: Vec<Note>| {
                notes.into_iter().map(|n| n.body).collect::<Vec<_>>()
            };
            let pinned: Vec<Note> =
                col.by_pinned(&db, &true).try_collect().await.unwrap();
            assert_eq!(by_body(pinned), vec!["keep"]);

            // save with a changed indexed field re-keys the record.
            col.save(
                &db,
                b,
                &Note {
                    body: "later".into(),
                    pinned: true,
                },
            )
            .await
            .unwrap();
            let pinned: Vec<Note> =
                col.by_pinned(&db, &true).try_collect().await.unwrap();
            assert_eq!(pinned.len(), 2);

            // remove de-indexes from the secondary too.
            assert!(col.remove(&db, a).await.unwrap());
            let pinned: Vec<Note> =
                col.by_pinned(&db, &true).try_collect().await.unwrap();
            assert_eq!(by_body(pinned), vec!["later"]);
        });
    }

    // The `#[wavedb::key(...)]` derive end-to-end: the anchor is derived
    // from the key field through core's one hash fn, insert is an upsert at
    // it, a save addressing a foreign anchor refuses typed, and a removed
    // key written again revives chaining onto its whole history.
    #[test]
    fn derived_natural_key_upserts_at_the_content_anchor() {
        use futures::TryStreamExt;
        use futures::executor::block_on;
        use wavedb_core::{NonUniqueStruct as _, U48};

        use super::Setting;

        let s = |value: &str| Setting {
            slug: "theme".into(),
            value: value.into(),
        };
        // The generated `natural_key`: SeaHash over the key field's wire
        // bytes — every build derives the same anchor from the same value.
        assert_eq!(
            s("dark").natural_key(),
            Some(wavedb_core::natural_key_hash(&wavedb_core::to_wire(
                &"theme".to_string()
            ))),
        );

        block_on(async {
            let store = mem::MemStore::default();
            let db = wavedb_core::LocalHandle::new(&store, U48::from(21u32));
            let pivot = Setting::create_pivot(&db).await.unwrap();
            let col = Setting::collection(pivot);

            // Insert twice = upsert: same id, the live state converges,
            // the superseded version chains.
            let a = col.insert(&db, &s("dark")).await.unwrap();
            let b = col.insert(&db, &s("light")).await.unwrap();
            assert_eq!(a, b, "one key value = one anchor");
            let all: Vec<Setting> = col.all(&db).try_collect().await.unwrap();
            assert_eq!(all, vec![s("light")]);

            // The identity IS the key: a save addressing `a` with another
            // slug refuses — renaming is remove + insert.
            let err = col
                .save(
                    &db,
                    a,
                    &Setting {
                        slug: "lang".into(),
                        value: "pt".into(),
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(err, wavedb_core::Error::KeyMismatch(_)));

            // A removed key written again revives at the same anchor,
            // chained onto its whole prior history.
            assert!(col.remove(&db, a).await.unwrap());
            let back = col.insert(&db, &s("dark again")).await.unwrap();
            assert_eq!(back, a, "same key, same anchor, through death");
            let versions: Vec<(wavedb_core::Metadata, Setting)> =
                col.history(&db, a).try_collect().await.unwrap();
            assert_eq!(versions.len(), 3, "v1, v2, and the revival chain");
        });
    }

    // The generated API end-to-end, in the exact shape application code uses:
    // an explicit `create_pivot`, then `collection(...)` driving
    // insert / all / save / remove — no raw `BpTree` anywhere.
    #[test]
    fn derived_collection_flow_end_to_end() {
        use futures::TryStreamExt;
        use futures::executor::block_on;
        use wavedb_core::U48;

        block_on(async {
            let store = mem::MemStore::default();
            let db = wavedb_core::LocalHandle::new(&store, U48::from(42u32));

            // A Unique record holds the collection handle (the owning record).
            let notes = Note::create_pivot(&db).await.unwrap();
            let about = AboutUser {
                name: "Ada".into(),
                city: "London".into(),
            };
            about.save(&db).await.unwrap();
            assert_eq!(AboutUser::get(&db).await.unwrap(), Some(about));

            // Drive the collection through the typed handle.
            let col = Note::collection(notes);
            let a = col
                .insert(
                    &db,
                    &Note {
                        body: "first".into(),
                        pinned: false,
                    },
                )
                .await
                .unwrap();
            let b = col
                .insert(
                    &db,
                    &Note {
                        body: "second".into(),
                        pinned: true,
                    },
                )
                .await
                .unwrap();

            let walked: Vec<Note> = col.all(&db).try_collect().await.unwrap();
            assert_eq!(
                walked.iter().map(|n| n.body.as_str()).collect::<Vec<_>>(),
                vec!["second", "first"],
                "most recently written first"
            );

            // Update = save at the stable Id; identity never changes.
            let mut second = walked[0].clone();
            second.pinned = false;
            col.save(&db, b, &second).await.unwrap();
            assert_eq!(col.get(&db, b).await.unwrap(), Some(second));

            // Remove drops it from the walk; bytes stay (history).
            assert!(col.remove(&db, a).await.unwrap());
            let after: Vec<Note> = col.all(&db).try_collect().await.unwrap();
            assert_eq!(after.len(), 1);
            assert_eq!(after[0].body, "second");
            assert!(col.get(&db, a).await.unwrap().is_some());
        });
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod lane_tests {
    use super::Note;
    use wavedb_core::index::Lane;

    #[test]
    fn lane_hashes_match_the_engines() {
        // The macro derives each lane's `STRUCT_HASH` at expansion time —
        // a `static StructStorage` needs a `const` initialiser and SeaHash is
        // not a `const fn` — while the engine derives it at runtime through
        // `Lane::hash`. Two implementations of one identity: if they drift, a
        // chain writes into a directory nothing reads and the failure is
        // silent, so it is pinned here against a real generated type.
        for (lane, slot) in [
            (Lane::Records, Note::records_lane_storage()),
            (Lane::Dead, Note::dead_lane_storage()),
            (Lane::Recency, Note::recency_lane_storage()),
            (Lane::Index, Note::index_lane_storage()),
        ] {
            assert_eq!(
                slot.struct_hash(),
                lane.hash(Note::STRUCT_HASH),
                "{lane:?} lane hash drifted between the macro and the engine"
            );
        }
    }

    #[test]
    fn the_lane_hashes_the_collision_guard_reads_are_the_real_ones() {
        // `WaveDbStruct::LANE_HASHES` is what the registry's salt guard
        // compares (a lane occupies a `type_salt` exactly as a record type
        // does). It is a *third* derivation of the same identity, so it is
        // pinned like the storage slots: a stale list would leave real
        // occupants of the 15-bit space unchecked, silently.
        use wavedb_core::WaveDbStruct as _;
        assert_eq!(
            Note::LANE_HASHES,
            [
                Lane::Records.hash(Note::STRUCT_HASH),
                Lane::Recency.hash(Note::STRUCT_HASH),
                Lane::Dead.hash(Note::STRUCT_HASH),
                Lane::Index.hash(Note::STRUCT_HASH),
            ],
            "the guard's lane list drifted from the engine's"
        );
        // A Unique type owns no collection, so it reserves no lane.
        assert!(
            super::AboutUser::LANE_HASHES.is_empty(),
            "{:?}",
            super::AboutUser::LANE_HASHES
        );
    }

    #[test]
    fn every_lane_a_collection_needs_is_registered() {
        // `PageStore::open` takes exactly this list; a lane missing from it
        // refuses at runtime with `no StructStorage registered`.
        let registered: Vec<u64> = Note::storage_entries()
            .iter()
            .map(|s| s.struct_hash())
            .collect();
        for lane in [Lane::Records, Lane::Dead, Lane::Index] {
            assert!(
                registered.contains(&lane.hash(Note::STRUCT_HASH)),
                "{lane:?} is not in Note::storage_entries()"
            );
        }
    }
}
