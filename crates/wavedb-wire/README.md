# wavedb-wire

A small, standalone **`WaveWire` codec**: turn a value into bytes and back.

```rust
let bytes: Vec<u8> = wavedb_wire::to_wire(&value);        // T       -> Vec<u8>
let value: T       = wavedb_wire::from_wire::<T>(&bytes)?; // Vec<u8> -> T
```

That is the whole job. The crate has **one** dependency (`thiserror`) — plus
`crc32fast` when the optional `validation` feature is on (below).

## The trait and the derive

`WaveWire` is the trait every encodable type implements; `#[derive(WaveWire)]`
generates it. The trait and the derive share the name (the same pattern as
`Clone`):

```rust
#[derive(wavedb_wire::WaveWire)]
struct Point { x: u64, y: u64 }
```

Built-in impls cover the common types: integers and floats, `bool`, `char`,
`String`, `Vec`, `Option`, arrays, and tuples. The derive handles:

- **structs** — named, tuple, or unit: fields encode in declaration order.
- **enums** — all variants field-less ⇒ a single `u8` tag; any variant with
  fields ⇒ `tag (u8) + payload length (u32)` plus the active variant's fields.
  Tags follow variant declaration order.

The derive emits absolute `::wavedb_wire::` paths, so it works from any crate that
depends on `wavedb-wire`. Its expansion lives in `wavedb-wire-codegen`, which
`wavedb-macros` shares (its paths go through `::wavedb_core::wire`).

## Views — reading without copying (RFC 0076)

Beside `Point`, the derive emits `PointRef<'a>`, its **borrowed twin**: the
same fields, each the field's view over the bytes — `&'a str` for a `String`,
`&'a [u8]` for a `Vec<u8>`, a `ListRef<'a, T>` for any other `Vec`, numbers
by value, nested twins for nested types.

```rust
let p: PersonRef<'_> = wavedb_wire::view_wire::<Person>(&bytes)?; // one checked walk
println!("{}", p.name);              // a &str into `bytes`, nothing allocated
let owned: Person = p.into();       // back to owned, for a write
```

- `view_wire` makes the walk `from_wire` makes and raises the same errors on
  the same bytes, but borrows where the decode copies, so reading the view
  afterwards cannot fail.
- `Wire<T>` owns one buffer (`Box<[u8]>`, plus where `T` starts in it), is
  `Send + Sync` whatever `T` is, and drops as one deallocation;
  `wire.view()` checks its bytes on each call, so view once and keep the view.
- `ListRef::get(i)` is O(1) when no element has heap bytes, O(i) otherwise.
- A twin is `Copy`. `From<NameRef> for Name` (`.into()`) gives the owned
  value back, and `TryFrom<Wire<Name>> for Name` (`.try_into()?`) the
  owner's. A field alone comes back with std's own `to_owned()` /
  `to_vec()`.
- A hand-written impl provides `Ref`, `SliceRef`, `view`, `from_ref`,
  `view_slice` and `from_slice_ref`; a type with no heap bytes that is
  `Copy + Debug` is its own view, and `wavedb_wire::own_view!()` writes them.

## Layout

A value is two contiguous parts:

```
[ STACK — T::STACK_SIZE bytes, fixed ][ HEAP — variable ]
```

Fixed-width fields pack little-endian into the stack at compile-time offsets;
dynamic fields (`String`, `Vec`, …) keep a `u32` length slot in the stack and put
their payload in the heap, so serialisation allocates once. Full spec in
[`docs/wire_format.md`](../../docs/wire_format.md).

## Feature `validation` — crc32-framed encoding

For bytes that cross an unreliable boundary (disk, transport), the `validation`
feature adds a checked pair that frames the plain encoding with a CRC32:

```rust
let bytes = wavedb_wire::to_wire_checked(&value);           // [crc32 4 B LE][stack][heap]
let value: T = wavedb_wire::from_wire_checked::<T>(&bytes)?; // verify crc, then decode
```

- The prefix is a little-endian `u32` CRC32 of the `[stack][heap]` payload;
  `CRC_PREFIX_LEN` (= 4) names the framing size. Encoding stays one allocation
  (the slot is reserved, then patched).
- A mismatch fails as `Error::CrcMismatch { stored, computed }` **before** any
  decode; a buffer shorter than the prefix as `Error::UnexpectedEof`.
- The two framings don't mix: plain `from_wire` on a checked buffer would read
  the CRC bytes as the value's stack. Pick one per boundary.
- Integrity only, not identity: the CRC says the bytes weren't corrupted, not
  that they encode the `T` you asked for (that stays the caller's job).

`wavedb-core` forwards the feature (`wavedb-core/validation`) and re-exports the
pair via `wavedb_core::wire`.

## What it does *not* do

- **The bytes carry no type tag.** `from_wire::<T>` does not check "is this really
  a `T`?" — it just reads a `T` out of the buffer. Picking the right `T` is the
  caller's job; this crate only moves bytes.
- **No validation beyond layout.** A decode fails only on a size/shape mismatch:
  the buffer is too short (`Error::UnexpectedEof`, the common case) or an intrinsic
  per-type check trips (`InvalidBool`, `InvalidChar`, `Utf8`, `InvalidTag`).
  Decoding the wrong type against a long-enough buffer yields some other value, not
  an error.
