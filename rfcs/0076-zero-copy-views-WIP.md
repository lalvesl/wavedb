# RFC 0076 — Zero-copy views over the wire

- **Status:** In progress (opened 2026-10-23: the design, before the code)
- **Grows out of:** the `docs/improvements.md` entry "Decoding copies every
  record out of its buffer — zero-copy views" (2026-10-18)
- **Builds on:** [RFC 0044](0044-page-cache.md) (the page cache holds decoded
  pages), [RFC 0068](0068-record-cache-as-a-log-WIP.md) (the record cache)
- **Crates:** `wavedb-wire`, `wavedb-wire-derive`, then `wavedb-storage`
  (`SlotPage`), `wavedb-core` (collection reads), `wavedb-macros`
  (`#[server]`)
- **Code:**
  - `crates/wavedb-wire/src/{lib,impls}.rs` (the trait, the built-in impls)
  - `crates/wavedb-wire-derive/src/{lib,enums}.rs` (the derive)
  - `crates/wavedb-storage/src/page.rs` (the first user)

## Summary

Every `WaveWire` type gets a **borrowed twin**, built in one pass over its
bytes: `#[derive(WaveWire)]` on `Thing` also emits `ThingRef<'a>`, whose
fields are `&'a str`, `&'a [u8]`, scalars by value, nested `PersonRef<'a>`,
and list views. `Wire<T>` is the owned buffer they borrow from: one
`Box<[u8]>`, `Send`, dropped as one deallocation.

`wire.view()?` validates once (bounds, UTF-8, tags) and hands back the
`ThingRef`, whose fields are then plain field reads. `r.to_owned()` gives
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
    type Ref<'a>;

    /// Build the twin from `STACK_SIZE` bytes of `stack` and this value's
    /// payload in `heap` — the walk `decode` makes, borrowing what it would
    /// copy. Every check `decode` makes happens here, once.
    fn view<'a>(stack: &mut Cursor<'a>, heap: &mut Cursor<'a>)
        -> Result<Self::Ref<'a>>;

    /// The owned value back: the copy `decode` would have made.
    fn from_ref(r: &Self::Ref<'_>) -> Self;
}
```

`view_wire::<T>(bytes) -> Result<T::Ref<'_>>` is the entry point, the twin
of `from_wire`. A hand-written impl (`Succession`) writes its own `view` and
`from_ref`. The derive writes the rest.

### 3. What each type borrows as

| Type | `Ref<'a>` | How |
|---|---|---|
| `u8..u128`, `i8..i128`, `f32`, `f64`, `bool`, `char` | the value itself | `from_le_bytes` on the stack slot, as `decode` |
| `String` | `&'a str` | the heap bytes, `str::from_utf8` once in `view` |
| `Vec<u8>` | `&'a [u8]` | the heap region as is |
| `Vec<T>`, `T` a fixed-width scalar | `Scalars<'a, T>` | the region, `len`/`get(i)`/`iter()` by `from_le_bytes` over `chunks_exact`, any alignment |
| `Vec<T>`, any other `T` | `ListRef<'a, T>` | the region and its element count, every element checked once in `view` |
| `Option<T>` | `Option<T::Ref<'a>>` | the flag, then `T`'s view |
| `[T; N]` | `[T::Ref<'a>; N]` | element by element |
| tuple | tuple of `Ref`s | member by member |
| struct (derive) | `NameRef<'a>`, one public field per field | the fields' views in declaration order |
| enum (derive) | `NameRef<'a>`, one variant per variant | the tag, then the variant's fields |

`ListRef::iter()` yields `T::Ref<'a>` and cannot fail, because `view`
already walked every element. Its `get(i)` is O(1) when `T` has no heap
(every unit is `T::STACK_SIZE` bytes) and O(i) otherwise. `Scalars` and
`ListRef` give their owned form back with `to_vec()`.

### 4. `Wire<T>`, the owner

```rust
pub struct Wire<T> {
    bytes: Box<[u8]>,
    start: u32,                 // where T's encoding begins, past any envelope
    _type: PhantomData<fn() -> T>,
}
impl<T: WaveWire> Wire<T> {
    pub fn new(bytes: Box<[u8]>, start: u32) -> Self;
    pub fn view(&self) -> Result<T::Ref<'_>>;
    pub fn bytes(&self) -> &[u8];
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

### 5. Back to owned: `to_owned()`

A write takes the owned `T`, as today. The derive emits two ways back:

- `impl NameRef<'_> { pub fn to_owned(&self) -> Name }`, an inherent method
  (it takes precedence over the blanket `ToOwned` a `Clone` `Ref` would
  have);
- `impl From<NameRef<'_>> for Name`.

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
     `view_wire`, and the derive's `NameRef<'a>` with `to_owned`/`From`.
   - Tests:
     - `from_ref(view(encode(x))) == x` for every built-in and a nested
       derive;
     - every decode error a view must also raise, asserted by variant
       (`UnexpectedEof`, invalid UTF-8, a bad tag).
2. **`SlotPage`.**
   - A page decoded from disk becomes its decompressed body and a sorted
     index of `(id, offset, len)`: two allocations instead of one per
     record. The page cache holds that.
   - A read copies one record's bytes out (the store's `Vec<u8>`, as
     today): one `memcpy`, independent of the page from then on.
   - A settle stages a page as that buffer plus its changes and merges them
     when it encodes. Nothing is decoded per record.
3. **Collection reads.**
   - `get`, `all` and the list readers gain a `Wire<T>`-returning form.
   - Whether it becomes the default is decided by a measurement.
4. **`#[server]`.** Arguments and replies cross threads as `Wire<T>`
   buffers.

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
  borrowed, with `to_owned()` between them.
- **The owned struct in one allocation.** Unsound by the allocator's
  contract (§4).

## Open questions

1. **O(1) indexing of lists of dynamic elements** (`Vec<String>`,
   `Vec<Struct>`). This needs an offset table in the format. Take it only if
   a measurement asks for it.
2. **The default read.** Whether `col.get` returns `Wire<T>` or keeps
   returning `T`, with `get_wire` beside it.
