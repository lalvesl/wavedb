# RFC 0016 — Server functions (`#[server]`)

- **Status:** Implemented
- **Crates:** `wavedb-macros`, `wavedb`, `wavedb-core` (`fn_identity`)
- **Code:** `#[server]` expansion; `core::fn_identity::compose`; `server.rs`

## Summary

`#[server]` turns one function declaration into three things: a **fn-type** (its
own `STRUCT_HASH` + dispatch), a **server body** retyped onto a node-side
`ServerDb`, and a **client stub** with the same signature. A call rides the
**same `CommandFrame`** in the **same hash space** as an object op — the function
arm just decodes `payload` as its argument tuple. This is WaveDB's answer to
"filtered / derived reads": there is no client query DSL — a filtered read is a
`#[server]` function.

## Motivation

Reads that are not "get by id" or "walk a collection" — filters, joins,
derivations — need to run **where the data is** (the node), not ship the dataset
to the client. And they must not require a parallel RPC/DTO layer
([RFC 0001](0001-vision-and-non-goals.md)). Making a server function *just
another entry in the one hash space* means it reuses the entire transport,
dispatch, and exposure machinery unchanged.

## Design

- **Composed identity.** The fn `STRUCT_HASH` is
  `fn_identity::compose(name_seed, [arg tags…, return tag])` — each `#[wavedb]`
  argument tags as its own `STRUCT_HASH`, so a schema change to an argument type
  *transitively* renames every function carrying it. A stream return composes
  under `STREAM_KIND` (a scalar and a stream of the same item are different
  functions). The mixer is a documented `const` SplitMix64 fold (not seahash —
  it must run in `const` context from other crates' consts).
- **Body retyped onto `ServerDb`.** The macro rewrites `&Db` → `&ServerDb<S>` so
  the same body text executes node-side against the local store (since
  [RFC 0067](0067-thread-per-core-shards-WIP.md) step 2: `&ServerDb<R>` over a
  `Route`, the body at the edge and each operation at its owner); auth/permission
  checks live **inside the body**, not the match ([RFC 0026](0026-auth-tokens.md)).
- **Client stub.** Same signature, sends the frame, decodes the reply.
- **Auth tiers.** A plain `#[server]` is login-required — the macro injects a
  guard that refuses `user == U48::MAX` before decoding. `#[server(public)]`
  opens the unauthenticated tier (`login`/`refresh`/`logout`).
- **Streaming returns.** `-> impl Stream<Item = Result<T>>` is detected; the body
  returns a stream against `ServerDb`, dispatch ships items over the framed wire
  ([RFC 0020](0020-net-transport-dumb-tunnel.md)), and the stub re-exposes the
  async iterator.

## Consequences

- **One frame, indistinguishable.** At the frame level you cannot tell a
  function call from an object op — only `match struct_hash` can. This is a
  deliberate security property ([RFC 0017](0017-exposure-registry-and-side-features.md)).
- **Storage-only helper types.** A `#[server]` body may touch types that are
  never wire-addressable (`store`-only exposure entries) — the body reads/writes
  them, the wire cannot name them.
