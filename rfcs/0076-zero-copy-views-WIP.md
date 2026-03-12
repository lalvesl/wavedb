# RFC 0076 — Zero-copy views over the wire

- **Status:** In progress (opened 2026-10-23; step 1 — the codec and the
  derives — landed 2026-10-23)
- **Grows out of:** the `docs/improvements.md` entry "Decoding copies every
  record out of its buffer — zero-copy views" (2026-10-18)
- **Builds on:** [RFC 0044](0044-page-cache.md) (the page cache holds decoded
  pages), [RFC 0068](0068-record-cache-as-a-log-WIP.md) (the record cache)
- **Crates:** `wavedb-wire`, `wavedb-wire-derive`, then `wavedb-storage`
  (`SlotPage`), `wavedb-core` (collection reads), `wavedb-macros`
  (`#[server]`)
- **Code:**
  - `crates/wavedb-wire/src/{lib,impls,compound,view}.rs` (the trait, the
    built-in impls, `ListRef`, `Wire<T>`, `view_wire`)
  - `crates/wavedb-wire-codegen/src/{lib,twin,enums}.rs` (the expansion both
    derives share)
  - `crates/wavedb-storage/src/page{,/codec}.rs` (the first user)

## Summary

Every `WaveWire` type gets a **borrowed twin**, built in one pass over its
bytes: `#[derive(WaveWire)]` on `Thing` also emits `ThingRef<'a>`, whose
fields are `&'a str`, `&'a [u8]`, scalars by value, nested `PersonRef<'a>`,
and list views. `Wire<T>` is the owned buffer they borrow from: one
`Box<[u8]>`, `Send`, dropped as one deallocation.

`wire.view()?` validates once (bounds, UTF-8, tags) and hands back the
`ThingRef`, whose fields are then plain field reads. `Thing::from(r)` gives
back the owned `Thing`, with its `String`s and `Vec`s, for a write. There
is no `Cow` anywhere.

**The format does not change.** A view reads the same bytes the decoder
reads, in the same order. It borrows where the decoder copies.

## Motivation

A read today ends in `from_wire::<T>`, which builds an owned `T`:

- every `String` and `Vec<u8>` is a copy of its bytes into an allocation of
  its own (one `memcpy` each since `7ee2042`);
- every `Vec<T>` is an allocation;
- a page body is one allocation per record (`SlotPage::records`, a
  `BTreeMap<u128, Vec<u8>>`), 391 of them in a micro page.

The read probe measured a page decode at 1.39 ms before the bulk codec,
1.1 ms of it the codec walking `Vec<u8>` byte by byte. The bulk `memcpy`
cut that to 0.34 ms, of which zstd is 0.27 ms. What is left of the codec's
share is allocation and copying of bytes the reader may never look at.

A decoded `T` is free to move between threads — `Send`, no `Rc` — and that
property is worth keeping. `#[server]` functions hand arguments and replies
between threads. A view has to keep it, which rules out sharing a page's
buffer by reference count.

## Design

### 1. The format stays

A dynamic field keeps its `u32` length slot in the stack section, and the
heap is still read in field order (`docs/wire_format.md`). A view is built
the way a decode is: one walk, each field's bytes taken from the cursor —
`Cursor::take` already returns `&'a [u8]` — and kept as a reference instead
of copied.

The improvements entry had planned to move each heap region's **offset**
into the stack, so that any field could be reached in O(1). That only pays
for lazy access, one field at a time, where a field is found and validated
when it is read. The eager view walks every field anyway, so the offsets
would buy it nothing. Lazy access is the alternative this RFC drops
([Alternatives](#alternatives)).

### 2. The trait

`WaveWire` gains a generic associated type and two methods:

```rust
pub trait WaveWire: Sized {
    // … STACK_SIZE, heap_size, encode_stack, encode_heap, decode, …

    /// The borrowed twin: what `view` builds over a value's bytes.
    type Ref<'a>: Copy + Debug;
    /// What a `Vec` of these borrows as: `ListRef`, or `&'a [u8]` for bytes.
    type SliceRef<'a>: Copy + Debug;

    /// Build the twin from `STACK_SIZE` bytes of `stack` and this value's
    /// payload in `heap` — the walk `decode` makes, borrowing what it would
    /// copy. Every check `decode` makes happens here, once.
    fn view<'a>(stack: &mut Cursor<'a>, heap: &mut Cursor<'a>)
        -> Result<Self::Ref<'a>>;

    /// The owned value back: the copy `decode` would have made.
    fn from_ref(r: Self::Ref<'_>) -> Self;

    /// The same pair for a `Vec` of these, over its whole heap region.
    fn view_slice(region: &[u8]) -> Result<Self::SliceRef<'_>>;
    fn from_slice_ref(r: Self::SliceRef<'_>) -> Vec<Self>;
}
```

- **`Copy`, so `from_ref` takes the view by value.** Every view is made of
  references, numbers and counts; passing one is copying a few words.
- **`SliceRef` is how `Vec<T>` picks its view per element type**, the way
  `decode_slice` already lets a scalar decode its `Vec` in bulk: `Vec<T>`'s
  `Ref<'a>` is `T::SliceRef<'a>`. Without it a `Vec<u8>` could not be
  `&[u8]`.

`view_wire::<T>(bytes) -> Result<T::Ref<'_>>` is the entry point, the twin
of `from_wire`. A hand-written impl writes its own items; one with no heap
bytes that is `Copy + Debug` (`U48`, `Succession`, `BlockDescriptor`) is its
own view, and `wavedb_wire::own_view!()` writes them. The derive writes the
rest.

### 3. What each type borrows as

| Type | `Ref<'a>` | How |
|---|---|---|
| `u8..u128`, `i8..i128`, `f32`, `f64`, `bool`, `char` | the value itself | `from_le_bytes` on the stack slot, as `decode` |
| `String` | `&'a str` | the heap bytes, `str::from_utf8` once in `view` |
| `Vec<u8>` | `&'a [u8]` | the heap region as is |
| `Vec<T>`, `T` a fixed-width number | `ListRef<'a, T>` | the region; the count is a division, nothing walked (any bytes are a number), and every read a `from_le_bytes`, any alignment |
| `Vec<T>`, any other `T` | `ListRef<'a, T>` | the region and its element count, every element checked once in `view` |
| `Option<T>` | `Option<T::Ref<'a>>` | the flag, then `T`'s view |
| `[T; N]` | `[T::Ref<'a>; N]` | element by element |
| tuple | tuple of `Ref`s | member by member |
| struct (derive) | `NameRef<'a>`, one field per field, each as visible as its own | the fields' views in declaration order |
| enum (derive) | `NameRef<'a>`, one variant per variant | the tag, then the variant's fields |
| a type with no fields anywhere (derive) | `NameRef`, the same shape, no lifetime | the tag, or nothing |

`ListRef::iter()` yields `T::Ref<'a>` and cannot fail, because `view`
already walked every element. Its `get(i)` is O(1) when `T` has no heap
(every unit is `T::STACK_SIZE` bytes) and O(i) otherwise. It gives its
owned form back with `to_vec()`. A separate `Scalars` type was planned for
lists of numbers; a `ListRef` built without the walk does the same job.

### 4. `Wire<T>`, the owner

```rust
pub struct Wire<T> {
    bytes: Box<[u8]>,
    start: usize,               // where T's encoding begins, past any envelope
    _type: PhantomData<fn() -> T>,
}
impl<T: WaveWire> Wire<T> {
    pub fn new(bytes: Box<[u8]>, start: usize) -> Self;
    pub fn encode(value: &T) -> Self;           // a buffer of its own
    pub fn view(&self) -> Result<T::Ref<'_>>;
    pub fn decode(&self) -> Result<T>;          // the owned value
    pub fn bytes(&self) -> &[u8];               // envelope included
    pub fn into_parts(self) -> (Box<[u8]>, usize);
}
```

- **One allocation, one drop.** A `String` field is a range of the buffer,
  not an allocation, so dropping a `Wire<T>` is one deallocation, whatever
  `T` holds. The borrow checker keeps it alive while any `Ref` borrows it.
- **`Send` and `Sync` whatever `T` is.** It holds only bytes, and
  `PhantomData<fn() -> T>` claims no `T`. Moving it to another thread moves
  a pointer.
- **`view` validates every call.** Without `unsafe` there is no way to hand
  out a `&str` the type system has not seen validated, so a caller views
  once and keeps the `Ref`. The walk is the decode's, without its copies.
- **`start` skips an envelope.** A stored record is
  `[STRUCT_HASH][meta_len][Metadata][body]`. Its `Wire` keeps the whole
  buffer as the store handed it over and views the body. The `Metadata` is
  viewed the same way when asked.

The owned struct cannot live inside the buffer instead. `GlobalAlloc::dealloc`
must receive exactly the pointer and layout `alloc` returned, so a `String`
carved out of a larger block is undefined behaviour when it drops. Views are
what makes the single allocation sound.

### 5. Back to owned: `From` and `TryFrom`

A write takes the owned `T`, as today. The derive emits two ways back:

- `impl From<NameRef<'_>> for Name`: `Name::from(r)`, or `r.into()`;
- `impl TryFrom<Wire<Name>> for Name` — fallible, since a `Wire`'s bytes
  are only checked when read: `wire.try_into()?`. Per type, because a
  blanket `impl<T> TryFrom<Wire<T>> for T` is an orphan; generic code calls
  `Wire::decode`.

**Not `ToOwned`.** The trait asks `Owned: Borrow<Self>` — a `&NameRef` out
of a `&Name` — and no `NameRef` lives inside a `Name` to point at (`str` and
`String` work because the `str` *is* the `String`'s buffer, its length
carried in the reference). It is taken anyway: a twin is `Clone`, so std's
blanket gives it `ToOwned` to itself. An inherent `to_owned()` returning
`Name` was tried in step 1 and dropped, as a method that looks like the
trait's and is not.

A field alone comes back with what Rust already has: `r.name.to_owned()`
for a `String`, `r.blob.to_vec()` for a `Vec<u8>`. A read that ends in a
write pays for its copies and allocations then, and only the read that
needs them pays.

### 6. Safety and portability

- **No `unsafe`.**
  - `&str` comes from `str::from_utf8`.
  - Scalars come from `from_le_bytes`.
  - Lists of scalars are `Scalars`, never `&[u32]`, which would need an
    aligned address and native byte order.
- **Portable.** The bytes are the format's, little-endian with no padding
  and no `usize`. A view reads them the same on x86_64, aarch64, wasm32 and
  a big-endian CPU. On a little-endian one, `from_le_bytes` is a plain load.
- **Nothing reaches disk.** `Ref` types and `Wire<T>` are read-side only, so
  they fold into no `STRUCT_HASH`.

### 7. Where it is used, in order

1. **`wavedb-wire` and the derive.**
   - The trait, the built-in impls, `Scalars`, `ListRef`, `Wire<T>`,
     `view_wire`, and the derive's `NameRef<'a>` with `From` back.
   - Tests:
     - `from_ref(view(encode(x))) == x` for every built-in and a nested
       derive;
     - every decode error a view must also raise, asserted by variant
       (`UnexpectedEof`, invalid UTF-8, a bad tag).
2. **`SlotPage`.**
   - A page decoded from disk becomes its decompressed body, kept as it
     is: one allocation instead of one per record. The page cache holds
     that. (The plan was the body plus a sorted index of `(id, offset,
     len)`; storing the body as columns made the index the body itself —
     see [Progress](#progress).)
   - A read copies one record's bytes out (the store's `Vec<u8>`, as
     today): one `memcpy`, independent of the page from then on.
   - A settle stages a page as that buffer plus its changes and merges them
     when it encodes. Nothing is decoded per record.
3. **Collection reads.**
   - `get`, `all` and the list readers **return** `Wire<T>` — no owned
     twin of each (decided 2026-10-24: most reads only look, and the one
     that writes converts, `.try_into()?`).
   - The node answers a `Get` with the stored body, nothing decoded or
     re-encoded, and a walk with framed bodies the same way.
4. **`#[server]`.** Arguments and replies cross threads as `Wire<T>`
   buffers. (Replies landed with step 3: `Wire<T>` is `WaveWire` with
   `T`'s bytes.)

## Progress

**Step 1 landed (2026-10-23): the codec and the derives.**

- **The trait and the built-ins** (`crates/wavedb-wire`): `Ref`, `SliceRef`,
  `view`, `from_ref`, `view_slice`, `from_slice_ref` on every impl;
  `ListRef`/`ListIter`, `Wire<T>`, `view_wire` and `own_view!` in `view.rs`;
  the compound impls moved to `compound.rs` to keep `impls.rs` in budget.
- **One expansion for both derives.** The derive in `wavedb-wire-derive`
  (paths through `::wavedb_wire`) and the one in `wavedb-macros` (through
  `::wavedb_core::wire`, which `#[wavedb]` uses for records, Pivots and
  Pivot ids) were two copies of the codec's codegen. Each now calls
  `wavedb-wire-codegen::expand` with its path, so the twin is written once;
  the `wavedb-macros` derive gains enums with it.
- **What the twin carries.** `Clone`, `Copy` and `Debug` are written by hand,
  so a type parameter is not asked to be `Clone` or `Debug` itself; the
  twin's bounds are the type's own, which already make every field
  `WaveWire`. A type with no fields anywhere (a unit struct, a field-less
  enum) has a twin of the same shape with no lifetime, and derives
  `PartialEq`, `Eq` and `Hash` too.
- **Found on the way:** a `Vec` of zero-byte units (`Vec<()>`) over a region
  that is not empty sent `decode_slice` round forever, since no unit used up
  any of it. Both walks now refuse it as `UnexpectedEof`.
- **Tests:** `crates/wavedb-wire/tests/views.rs`, which checks
  `from_ref(view(encode(x))) == x` for every built-in and derived shape (a
  generic one included), that views borrow from the buffer (pointer ranges),
  every decode error raised alike by the view, `ListRef` indexing, and
  `Wire<T>` past an envelope and across a thread. Also
  `crates/wavedb-macros/tests/wire_derive.rs`, for the twin through the
  `wavedb_core::wire` paths.

**Step 2 landed (2026-10-23): `SlotPage` is its body and its changes.**

- **The body is columns.** `PageBody { ids: Vec<u128>, ends: Vec<u32>,
  data: Vec<u8> }` — the ids ascending, where each record ends, the records
  back to back — instead of `Vec<(u128, Vec<u8>)>`. Its view is O(1) (a
  list of numbers is counted by a division, `data` is a slice), and record
  `i` is `data[ends[i-1]..ends[i]]`, so the decompressed buffer *is* the
  index: a page opened from disk is **one allocation**, a lookup a binary
  search over the id column's bytes. The page format changed (no format
  versioning pre-release); the codec's did not.
- **Checked once, on open.** `from_bytes` checks the columns agree (as many
  ends as ids, ids strictly ascending, ends never going back, the last one
  the data's length), so every read after it is in range.
- **Writes are changes beside the body**, a map of `Some(bytes)` puts and
  `None` removals read first; iteration merges the two in id order.
  `to_bytes` now takes `&mut self` and **seals**: the merged body it
  encodes becomes the page's body, so a page a settle writes and the cache
  then holds carries no per-record allocations either.
- **The envelope is viewed, not decoded** (`view_wire_checked`, new in
  `wavedb-wire`): a compressed payload is decompressed straight from the
  read buffer, where the decode first copied it out.
- **`to_owned()` gave way to `From`/`TryFrom`** (§5), and the derive emits
  `TryFrom<Wire<Name>> for Name`.
- `SlotPage::remove` returns whether the record was there, not a copy of
  it; `into_entries` is gone (its three callers, a split, an eviction
  restore and an upload, iterate and copy what they keep).

**Step 3, first half (2026-10-24): `get` returns `Wire<T>`.**

- **Every `get`** — `T::get(&db)` for a Unique, `col.get(&db, id)` for a
  NonUnique, through all four `DbHandle`s (local, routed on the node, the
  client, `ServerDb`) — returns `Option<Wire<T>>`: the record's buffer as
  the store or the frame handed it over, its head and metadata checked, its
  body checked when viewed. There is no `get_owned`: `.view()?` reads,
  `.try_into()?` owns.
- **The node sends the stored body** for a `Get` (`expose::get_value`,
  `node_op`): it used to decode the record and encode it again.
- **The client's cache mirror compares bytes.** A Unique `Get` mirrors into
  the cache only when the cached body differs, and decodes only then
  (`client_cache::mirror_unique_wire`); a body that does not decode still
  fails the read, as before.
- **`#[wave_wire(own_view)]`** makes a `Copy` type with no heap bytes its
  own view, with no twin: `Id`, `LocalId`, and every generated
  `{Name}PivotId` — so `user.view()?.orders` is the handle itself, not a
  twin to convert.
- **`DbHandle::Error` takes wire faults** (`From<wire::Error>`, through
  `core::Error::Wire`; `wavedb::Error` gained the conversion), so
  `wire.view()?` works in any context, a `#[server]` body included.
- `Wire` is in `wavedb::prelude`; it gained `body()` and `into_body()`.
- **The bench reads what it fetched**: the micro `read` and the shop
  `profile` view the record (every field checked), as the other systems
  fetch a row's columns; the shop's order reads take the user's orders
  handle off a view, copying nothing else out.

**Step 3, second half (2026-10-24): the walks return `Wire<T>` — and
step 4 with them.**

- **Every walk** — `all`, the list readers (`listed_by_*`, `_at_page`),
  `by_*` / `by_*_range`, `history`, `fuzzy_*` — yields `Wire<T>` (with its
  `Id` or `Metadata` where it did), one buffer per record: the record as
  the store handed it, or one copy out of a list segment that holds it
  inline. A page owner shared by a whole segment is open question 3.
- **A walk's item frame is `(head, body)`** (`expose::framed` /
  `unframed`): the head — `(Id, Metadata)`, or a version's `Metadata` —
  then the record's body bytes, last. A tuple `(Id, Metadata, T)`
  interleaves `T`'s stack with the head's heaps; with the body last it is
  the frame's tail, and the client's `Wire` is the frame itself, viewed past
  the head. The node frames bodies as stored, decoding nothing.
- **The client decodes only to mirror.** A walk mirrors each record into
  the local cache when there is one, and only then decodes it.
- **`Wire<T>` is `WaveWire`, with `T`'s exact bytes** (`wire_codec.rs`):
  `Vec<Wire<T>>` is byte for byte a `Vec<T>`, and its `FnArgTag` is
  `T`'s. So a `#[server]` function returns what it read —
  `Result<Vec<Wire<Contact>>>` — and the records go out as stored and come
  in as buffers to view: step 4, for replies. Decoding one views a `T` to
  learn where its bytes end, then keeps exactly those.
- **`decode` takes one lifetime** for both cursors, as `view` does — they
  always come from one buffer — so a `Wire<T>` can view the `T` it decodes.

**Measured (2026-10-24, criterion, 2.3 GHz pinned).** Two benches:
`crates/wavedb-wire/benches/views.rs` (decode against view) and
`crates/wavedb-storage/benches/page.rs` (a micro page — 391 `Thing`
records — read from its bytes, the file running unchanged at the commit
before step 2 to compare).

| wire | decode | view |
|---|---:|---:|
| micro `Thing` (~260 B) | 152 ns | 79 ns |
| `Doc` (string, 1 KiB bytes, 16 strings) | 1.02 µs | 347 ns |
| `Vec<u8>` 4 KiB / 64 KiB | 153 ns / 2.68 µs | 2.7 ns / 2.7 ns |
| `Vec<u32>` ×1000 (view, then summed) | 128 ns | 4 ns (896 ns) |
| `Vec<String>` ×100 (view, then each read) | 10.0 µs | 1.5 µs (3.3 µs) |
| a reply of 100 `Thing`s: `Vec<T>` / `Vec<Wire<T>>` (each then viewed) | 34.3 µs | 20.9 µs (29.1 µs) |

| page (391 records) | before step 2 | now |
|---|---:|---:|
| `from_bytes`, zstd | 215 µs | 152 µs |
| `from_bytes`, raw | 67 µs | 17 µs |
| `get` of every record | 16.0 µs | 18.5 µs |
| `iter` | 1.1 µs | 2.3 µs |

- **The bench found a regression step 2 had made, now fixed:** a page's
  `get` viewed its whole body each call (22 ns) and probed the id column
  through `ListRef::get` (~6 ns a probe for a `u128`), 57.8 µs for the
  page's gets; and `iter` merged through a `Peekable` that carried each
  record ahead of its turn, 6.0 µs. The page now keeps where its columns
  sit (`Cuts`, found by the one view `from_bytes` makes) and reads ids and
  ends in place (`as_chunks`, `from_le_bytes`, `binary_search_by`), and
  the merge peeks the body by id alone. What is left against the old
  `BTreeMap` — ~6 ns a `get`, ~3 ns a record walked — is noise beside the
  50–60 µs a page's open saves.
- **Iterating a list of numbers is slower than decoding it** (896 ns
  against 128 ns for 1000 `u32`s): decode is one `memcpy`, a `ListRef`
  walk views each element through cursors. A view still wins when a read
  touches few of them. Reading every string of a `Vec<String>` view checks
  each one's UTF-8 a second time (the walk the view made, then the read) —
  safe code cannot skip it, and it still beats the decode 3×.

Next: the deferred system benches, then open question 3 if they ask.

## Alternatives

- **`rkyv`.** Zero-copy archives with relative pointers, `String` and `Vec`
  included, and cheap to read. It would replace `WaveWire`: its own derive,
  and a format defined by its versions (0.7 to 0.8 changed it). The bytes on
  disk are this project's and fold into the `STRUCT_HASH`.
- **Lazy accessors** (`wire.name()`, each field found and validated when
  read) need the offsets in the stack, so a format change, and they
  revalidate a string on every access. The eager view validates once and
  then costs a field read. Lazy access would win only for wide records read
  for one field. The page index (step 2) already covers the page's version
  of that.
- **The `zerocopy` crate.** Unaligned little-endian scalar types and safe
  casts of fixed layouts. With the format unchanged and views eager, a
  scalar is one `from_le_bytes` and a scalar list is `Scalars`, so it would
  add a dependency to the public API for nothing the view needs. It is worth
  revisiting only if a fixed `#[repr(C)]` header ever appears.
- **`Cow` fields** in the declared struct, borrowed on read and owned on
  write. They put a lifetime on every user type and make a write's copies
  implicit. The split here keeps the declared struct owned and the twin
  borrowed, with `From` between them.
- **The owned struct in one allocation.** Unsound by the allocator's
  contract (§4).

## Open questions

1. **O(1) indexing of lists of dynamic elements** (`Vec<String>`,
   `Vec<Struct>`). This needs an offset table in the format. Take it only if
   a measurement asks for it.
2. ~~**The default read.**~~ Settled: a read returns `Wire<T>`, and there
   is no `get_owned` (step 3 in [Progress](#progress)).
3. **One owner per page for the walks.** `all` and the list readers return
   one `Wire` per record, one allocation each; a page's records could share
   one buffer the caller holds, viewed record by record. Later, if a
   measurement asks.
