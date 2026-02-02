# RFC 0053 — Tenant-fair cache retention

- **Status:** Planned — opened 2026-07-29
- **Crates:** `wavedb-storage`
- **Related:** [RFC 0044](0044-page-cache.md) is the *mechanism* this
  RFC sets policy for; [RFC 0051](0051-ordered-record-lists.md) and
  [RFC 0052](0052-segment-size-as-the-pagination-unit.md) are the
  structures whose cold cost was designed small **because** this policy cannot
  promise them residency

## Summary

WaveDB is multi-tenant, so **no structure may be pinned in memory.** A cache that
kept one tenant's index resident would hold RAM every other tenant needs, and the
first tenant to touch a large collection would evict everyone else. Today's
behaviour is the honest baseline: a read deposits into the cache, the cache
flushes on its own schedule, a hit is free and a miss is a read — *it happens*.

This RFC is about the layer above that: deciding **which entries deserve to stay
hot, without letting one tenant monopolise the budget.** It is a retention policy,
not a new cache, and it is deliberately separate from the structures that benefit —
those must be correct and bounded when nothing is cached at all.

## Motivation

The three RFCs 0050–0052 lean on small structures — a two-node sparse-index
descent, a segment that is one seek. None of them may lean on *residency*: quoting
a warm number as the design's cost would be describing a single-tenant database.

But "never pin anything" is not the same as "retain nothing useful". The access
pattern of a paginated application is extremely skewed: the root and first level
of a sparse index are touched by every query against that collection, while the
segments themselves stream past once. A policy that treated both alike would evict
the two nodes that serve every request in order to hold rows nobody will look at
again. That is the waste worth fixing — and fixing it *is* a policy question,
because the moment retention becomes selective, "selective in whose favour?"
becomes a multi-tenant fairness question.

## Design sketch

Deliberately a sketch: the mechanism should be chosen against a measured
workload, not argued into place. What this RFC fixes now is the shape of the
problem and the constraints any answer must satisfy.

**Constraints.**

1. **No pinning, ever.** Any entry must be evictable under pressure, whatever its
   heat. A "keep hot" hint is a preference the cache may ignore, never a lock.
2. **Per-tenant fairness, not per-key.** The budget is shared; a tenant scanning a
   million records must not evict a hundred other tenants' index roots. So
   accounting is per tenant, and eviction picks a victim *within* the tenant that
   is over its share.
3. **Bounded cold cost stays the contract.** The policy may make things faster; no
   design above may become *correct* only when it is warm.
4. **Cheap accounting.** Heat tracking that costs a lock per read, or a per-key
   timestamp, would spend the resource it is trying to save.

**Candidate shape.** Two classes of cached thing, with different retention:

- **Navigational** — index nodes, sparse-index roots, page-directory state: small,
  reread constantly, high value per byte. Retained by frequency.
- **Streaming** — record segments pulled by a scan: large, touched once, near-zero
  value per byte after the scan passes. Retained by recency, and a scan should be
  able to say so (a hint that its reads are single-use, so a listing cannot flush
  the whole cache — the classic scan-resistance problem).

That split, plus per-tenant accounting, is probably most of the value; whether it
wants a proper admission policy is an empirical question.

## Open questions

- **What is a tenant's share?** Equal split, proportional to live bytes, or
  demand-driven with a floor? Equal is defensible and trivial; proportional
  rewards the biggest tenant with the most cache, which may be right or exactly
  wrong depending on who pays.
- **Where does the accounting live?** `wavedb-storage` sees `Id`s, and an `Id`
  carries its `TENANT` in 48 bits — so per-tenant attribution is already available
  without a new lookup. Worth confirming that holds for every cached kind
  (directory state is per-`STRUCT_HASH`, not per tenant).
- **Does the scan hint reach the storage layer?** `Collection::all` knows its reads
  are single-use; `Store` has no way to say so today. This is the one part that
  might touch the `Store` seam.
- **Is any of this needed before a measured problem exists?** The baseline — read,
  deposit, flush, and accept the miss — is correct and already shipped. This RFC
  should stay *Planned* until a real workload shows the eviction of navigational
  state costing something.
