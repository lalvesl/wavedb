# WaveDB Development Standards

The rules this workspace is written to. Most of them are **enforced by
tooling** — the sections below say where each lives, so "is this compliant?"
is answerable by running the checks, not by taste.

The pre-commit bar, always:

```sh
cargo fmt --all
cargo clippy --workspace --all-targets   # zero warnings
cargo test --workspace                   # all green
```

---

## Toolchain

- **Pinned toolchain**: `rust-toolchain.toml` (currently `1.99.0`, with
  `wasm32-unknown-unknown`). Never build against "whatever is installed";
  `rust-version` in `[workspace.package]` must match the pin.
- **Edition 2024** for every crate, via `edition.workspace = true`.
- Every crate inherits `[workspace.lints]` (`[lints] workspace = true`) and
  workspace dependency versions — no per-crate version drift.

## Formatting

`rustfmt.toml` is the format; `cargo fmt --all` is the arbiter. The narrow
`max_width` it sets is deliberate (side-by-side diffs, review on narrow
panes). Don't fight it with `#[rustfmt::skip]` outside of byte-layout tables
where alignment carries meaning.

## Complexity budgets (enforced in `clippy.toml`)

The workspace runs clippy `pedantic` + `nursery` at `warn`, and warnings are
treated as failures at review time — so these lints are live limits:

- **`cognitive_complexity`** — the branching + nesting a reader must hold.
  Clippy's successor to the old cyclomatic metric.
- **`too_many_lines`** — lines of code per function (comments and blanks
  excluded).
- **`too_many_arguments`** — arguments per function.

**The numbers live in `clippy.toml` only** — that file is the single source
of truth for the thresholds; this document deliberately doesn't repeat them.

Rules around them:

- **Ratchet rule**: each threshold sits at the workspace's current ceiling
  (the offending functions are named in `clippy.toml`'s comments). When a
  ceiling shrinks, lower the threshold to match. Thresholds only ever go
  **down**.
- A function that grows past a budget gets **split, not allowed**. Extract
  the self-contained phase (a split-propagation loop, a decode helper) into a
  named private fn — the name documents the phase.

### Lint escape hatch

`#[allow]` is acceptable only in the existing workspace pattern: a **scoped**
allow (file or item level) with a comment stating *why the lint is wrong
here*, like the byte-packing casts in `wavedb-storage` or `future_not_send`
on the `Store`-generic seams. Never a bare `#[allow]`, never crate-wide
convenience allows, and complexity lints are not allowable at all — split the
function instead.

## Module & file shape

- Every module opens with a `//!` header stating its **one responsibility**
  (see any file in `wavedb-storage/src/`). If the header needs "and", the
  module wants splitting.
- **Hard file-length budget: 350 non-test lines per `.rs` file**, enforced by
  `scripts/check_file_length.sh` in CI (clippy has no file-level lint — its
  `too_many_lines` is per function). Tests colocate and don't count: the
  script stops counting at the first `#[cfg(test)]`. A file over budget
  splits by layer (e.g. a codec apart from the thing it encodes).
- One public type/concept per module as the default; `lib.rs` re-exports the
  public surface explicitly (no `pub use module::*`).

## Error handling

The pattern established across `wavedb-wire` / `wavedb-core` /
`wavedb-storage`:

- **One `thiserror` enum per layer**, named for the layer
  (`wavedb_wire::Error`, `wavedb_core::Error`, `StorageError`), each with a
  `Result<T>` alias.
- **Variants are typed and carry evidence** — the id that dangled, the tag
  found, `need`/`have` byte counts. A `&'static str` payload (`Corrupt`) is
  the floor; a `String`-payload variant is not a place to park new errors.
- **Fabricating a foreign layer's error is forbidden.** Storage code raises
  `StorageError::…` and lets the documented seam
  (`From<StorageError> for wavedb_core::Error` → `Backend`) flatten it at the
  `Store` boundary — never `wavedb_core::Error::Backend("some string".into())`
  inline.
- Cross-layer flow is `#[from]` chains + `?`, conversion at the boundary,
  once.
- **No `unwrap`/`expect`/`panic!` in library code paths.** Allowed in tests,
  and for genuinely infallible cases with the invariant stated in a comment
  (e.g. a `try_into` on a slice whose length was just checked).
- Every fallible `pub fn` documents its failure modes in an `# Errors`
  section (the `missing_errors_doc` allow exists for the trivial cases, not
  as license to skip the interesting ones).

## API design invariants

Project-defining rules — breaking one is an architecture change, not a
refactor:

- **No `dyn`, no sum-type registries.** Dispatch is a generated `match` on
  `STRUCT_HASH` to monomorphized arms.
- **No serde.** Byte layouts are `WaveWire`, defined in `docs/wire_format.md`,
  platform-independent (`usize`/`isize` never encodable).
- **`async` end to end** on every public surface; native = Tokio, wasm =
  `wasm_bindgen_futures`. No blocking I/O behind an async signature.
- **Identity-load-bearing dependencies are pinned exactly** (`=x.y.z`; see
  the `seahash` entry in the workspace `Cargo.toml`): if an algorithm's
  output is persisted or shared across builds, an unreviewed bump is
  corruption. Comment the pin with the reason.
- **Features are additive and off by default** (`validation` is the
  template): optional dep behind `dep:`, gated items `#[cfg(feature)]`-marked
  in docs, downstream crates forward the feature by the same name.
- **Schema crates never leak a side.** Every crate that expands `#[server]` /
  `expose_server!` / `expose_client!` declares two cargo features named
  exactly **`server-side`** and **`client-side`**; the macros gate their
  emission on them (`#[server]` bodies + dispatch + `expose_server!` under
  `server-side`; client stubs + `expose_client!` under `client-side`; the
  fn-type's `STRUCT_HASH` under both — identity is the protocol). A deployed
  binary depends on the schema with `default-features = false` and exactly
  its side, so the other side's logic is **never compiled** into the
  artifact — the guarantee is the `cfg`, not LTO/dead-code stripping.
  Hand-written server-only helpers (anything outside a `#[server]` body that
  serves one) carry `#[cfg(feature = "server-side")]` themselves, and
  `expose_server!` refuses (`compile_error!`) a wasm32 build with
  `server-side` on. Deliberate divergence from the additive template: both
  sides default **on**, so a schema crate's own tests drive the full loop —
  deployments opt *down* to one side.

## Testing

- Unit tests colocate in a `#[cfg(test)] mod tests`; cross-module behaviour
  goes in `tests/` (e.g. `nonunique_collection.rs`).
- Codecs get **roundtrip tests plus the failure cases** (truncation, bad tag,
  tampering) — asserting the *specific* error variant, not just `is_err()`.
- Storage changes need a **durability angle**: reopen-and-replay or
  kill-during-write coverage, not just the happy path.
- Test names state the behaviour (`tampered_payload_is_crc_mismatch`), not
  the method under test (`test_from_wire_2`).
- A bug fix lands with the test that would have caught it.

## Dependencies

- Versions live in `[workspace.dependencies]`; crates select with
  `.workspace = true`. Adding a dep to a crate means justifying it in a
  manifest comment (see every existing entry).
- `wavedb-wire` stays minimal (thiserror + optional crc32fast) — it is the
  everything-depends-on-it crate.
- `cargo deny` gates licenses and advisories (`deny.toml`); a new dep must
  pass it.

## Performance

Storage hot-path changes record a baseline before merge:

```sh
cargo run -p wavedb-bench --release --bin record-perf
```

and commit `crates/wavedb-bench/results/` so regressions are visible in
review (`results/README.md` has the scenarios).

## Commits & docs

- **Conventional commits**: `feat:` / `fix:` / `docs:` / `refactor:` /
  `perf:` / `test:`, imperative subject. Body explains *why* when the diff
  doesn't.
- READMEs in this repo describe the **target** design; when a documented
  mechanism isn't built yet, the doc says so explicitly (see the
  `> Status:` blocks). Keep that honesty when editing docs — and add or update
  the relevant RFC in `rfcs/` when an idea lands or changes (the RFC corpus is
  the design + progress record; process in `rfcs/0000-rfc-process.md`).
