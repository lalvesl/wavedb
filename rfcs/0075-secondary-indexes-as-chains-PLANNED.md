# RFC 0075 — Secondary indexes as chains

- **Status:** Planned (opened 2026-10-17)
- **Builds on:**
  - [RFC 0050](0050-clustered-record-chains.md) (the chain and its sparse
    index; B+trees opt-in);
  - [RFC 0051](0051-ordered-record-lists.md) (declared lists);
  - [RFC 0054](0054-no-duplication-by-default.md) (pointer chains);
  - [RFC 0074](0074-extendible-hashing-PLANNED.md) (a page per segment).
- **Revises:**
  - [RFC 0011](0011-bptree-index-and-collections.md) (the B+tree's last users);
  - [RFC 0056](0056-fuzzy-string-search-WIP.md) (the posting structure).
- **Crates:** `wavedb-core`, `wavedb-macros`, `wavedb-storage` (the reserved
  slot goes)
- **Code:**
  - `crates/wavedb-core/src/{collection_roots,collection_reindex,collection_read,collection_write,collection_remove,collection_keyed,collection_fuzzy*}.rs`
  - `crates/wavedb-core/src/index/{chain*,tree*,node*}.rs`
  - `crates/wavedb-macros/src/{secondaries,lists,declarations}.rs`

## Summary

A `#[wavedb::pivot(f)]` secondary index becomes a `Chain<()>`, the same
structure as the recency chain:
- segments of `[field bytes][anchor]` keys with no payload;
- a sparse index above them;
- three permanent roots in the Pivot.

`#[wavedb::fuzzy]` posting trees become the same chain. With that, the B+tree
has no user left, and it is deleted along with its reserved storage slot.

A `#[wavedb::list(f)]` is then the same chain holding the record inline. So a
pivot and a list on one ordering are one structure that differs only in its
payload:
- A list **also answers `by_f`**, without a per-anchor read.
- Declaring both on the same ordering is a compile error.

## Motivation

The settle-round probe on the micro workload at 60 000 records measured the
B+tree as the largest single cost of a durable insert: **110 of ~213 KiB**.
Two things in it are the B+tree's own:

- **The leaf is the unit of rewrite.** `DEFAULT_LEAF_CAP = 1819` makes a leaf
  ~28 KiB, and every insert rewrites a whole leaf to add one ~30-byte key.
  RFC 0074 stops the bucket around it from multiplying that cost, but the leaf
  itself stays.
- **The root moves.** A root split rewrites the Pivot. The `Pivot` trait
  already notes that this is the last thing that still does so.

The chain answers both:
- A pointer segment holds 256–512 entries. The recency chain's measured
  segments are ~6 KiB.
- The endpoints and the index root are permanent (RFC 0050).
- Range reads are what it is shaped for: one sparse-index descent finds the
  first segment, and `next` links walk the rest. This is the "between 10 and
  20" a secondary exists to answer.

The code already expects this outcome: `search_by`'s doc says the secondary
"stays one" B+tree only until lists replace it.

A dense secondary is inherent to unclustered data. Every anchor in range
still costs a `get`, and only clustering the records by the field avoids it.
That is exactly what a list is. So the honest surface is a pivot for "find
them" and a list for "find them and read them together", over one structure.

## Design

### The structure

- **Key:** `SecKey { field: value.secondary_key(i), rec: anchor }`, unchanged.
  The tie-break is the anchor, so a save relocates an entry only when the
  field changed. That rule stays as `plan_secondary_rekeys` has it today.
- **Payload:** `()`. The anchor is in the key, and the record lives there
  (RFC 0054).
- **Lane:** a new `Lane::Secondary` (`b"WDB.SEC"`). A field-keyed pointer is
  different content from an instant-keyed one, and a lane is what lets one
  directory and one dictionary model one kind of content.
  - Fuzzy postings get `Lane::Fuzzy` (`b"WDB.FZY"`).
  - The sparse nodes stay in `Lane::Index` with the other chains' nodes.
- **Capacity:** `DEFAULT_DEAD_MIN` (256…512 entries), like the recency chain.
  There are no records in it to paginate.
- **Roots:** `Pivot::secondaries()` returns `&[ChainRoots]`, and so does
  `fuzzy()`, in place of `&[LocalId]`. The Pivot's shape changes, so its own
  `STRUCT_HASH` changes with it.

### Writes

Every write stays inside the operation's one atomic `apply` batch, through the
overlay, the way the list planners already do it:
- an insert does `plan_insert(key, ())`;
- a save that changed the field does `plan_remove(old)` then
  `plan_insert(new)`;
- a remove does `plan_remove`.

The fuzzy planners do the same per gram that moved. No root ever moves, so no
secondary write ever rewrites the Pivot.

### Reads

`Chain` gains a seek:

```rust
/// Entries with keys ≥ `from`, ascending: one sparse-index descent to the
/// covering segment, then `next` links.
pub fn scan_from<'a, S: Store>(self, store: &'a S, from: SecKey)
    -> impl Stream<Item = Result<(SecKey, P)>> + 'a;
```

`search_by(index, bound)` becomes a scan from the bound's least key, with
the least `rec` (`LocalId::default()`), taken while `bound.contains(&key.field)`:
- `Bound::All` starts at the head;
- `Exact`, `Prefix` and `Range` start at `lo`.

Each anchor is then resolved with `get_of`, as today. The fuzzy read's
`Prefix` scan per gram is the same call.

`Bound::created_at_range` was only the tree's descent pruning, so it goes.

The sparse index already carries per-segment counts, so a range's size and a
filtered page ("matches 40–60") fall out of two descents. A rank-by-key step
is the only new primitive needed.

### One structure for pivots and lists

The macros generate the following:

| declaration | chain | `by_f` reads | `listed_by_f` |
|---|---|---|---|
| `pivot(f)` | `Chain<()>`, Secondary lane | anchors, then one `get` each | — |
| `list(f)` | `Chain<Vec<u8>>`, Records lane | the records inline | yes |
| both | **compile error**: the list already serves `by_f` | | |

- `by_f` takes the exact value(s), as today.
- A new `by_f_range(lo, hi)` (half-open, over `IndexKey` bytes) is generated
  for both kinds. It is the range read the secondary exists for.
- Composite orderings work unchanged, since `(f1, f2)` concatenates order-
  preserving encodings. A prefix on `f1` is a `Bound::Prefix`, and strings are
  `0x00`-terminated, so the prefix is exact.

### Identity

`declarations.rs` folds nothing for a secondary today, on the grounds that it
is "a derived lookup structure, not a fact about the bytes stored". That
reasoning does not survive the chain:
- index *i* of `Pivot::secondaries()` names a chain whose stored keys are
  field *i*'s encoding;
- reordering or retyping the declarations would point `by_f` at another
  field's chain and return wrong answers, not a refusal.

Under CLAUDE.md's rule, that is a stored-bytes fact. So `pivot` declarations
fold as synthetic `#pivot` entries, in declaration order, appended after the
fuzzy fold.

## Phases

1. **`Chain::scan_from`.** Tests:
   - a bound inside one segment, and one across many;
   - a bound below the head and one past the tail;
   - `All` and an empty chain;
   - a seek landing exactly on a separator.
2. **Secondaries on chains.**
   - Add `Lane::Secondary`, and make `Pivot.secondaries` a list of
     `ChainRoots`.
   - Move create, insert, reindex, remove and the keyed path onto chains, and
     run `search_by` through `scan_from`.
   - Fold `#pivot`.
   - Port the existing secondary tests.
   - Add a reopen test over a `PageStore` directory.
3. **Fuzzy on chains.**
   - Add `Lane::Fuzzy`, and move the posting planners and the gram scan onto
     chains.
   - Delete `BpTree`, `tree*.rs`, `node*.rs`, `BPTREE_NODE_STRUCT_HASH`,
     `BPTREE_NODE_STORAGE` and `Bound::created_at_range`.
4. **Pivot and list as one.**
   - Make a list answer `by_f`, and generate `by_f_range` for both.
   - Refuse both declarations on one ordering at compile time.
   - Drop the "neither is sugar for the other" docs in `lists.rs`.
5. **Measure.**
   - Re-run the probe and the micro rows after RFC 0074, and update
     `benches/results/index.md`.
   - Update CLAUDE.md and the core README, which still say "the
     `Store`-generic `BpTree<K>` index".

No format version is bumped and nothing is migrated, per policy.

## Alternatives

- **Keep the B+tree, fix only the hashing (RFC 0074).** A ~28 KiB leaf is
  still rewritten per insert, and a root split still rewrites the Pivot.
- **Smaller leaves.** Shrinking the leaf toward a segment's size and pinning
  the root's id would re-derive the chain and its sparse index, which already
  exist and are tested.
- **Every index a list, by the engine.** This answers `by_f` without
  per-anchor reads, but as a default it would make every declared index a full
  copy of the records, against RFC 0054. It is not lost, only left to the
  developer: since both kinds are one chain, swapping `pivot(f)` for
  `list(f)` on any ordering, or on all of them, is a one-word change with the
  same `by_f` surface. It trades a copy of every record per declaration for
  reads with no `get`, and because the declaration folds into the
  `STRUCT_HASH`, the swap yields a new type.
- **Pivot and list coexisting on one ordering (today).** The pivot then
  costs writes and answers nothing the list cannot answer faster.
