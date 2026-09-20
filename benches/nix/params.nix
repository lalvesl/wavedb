# The benchmark's size knobs, in one place because five derivations and both
# apps have to agree on them.
#
# ## Why a table and not two numbers
#
# A row's identity carries the **tier name**, never the row count (RFC 0065
# §1): `small` is what a stored measurement says it was measured against. So
# the mapping from name to count has to exist exactly once, or two runs can
# file different datasets under the same name and the corpus cannot tell them
# apart — which is precisely what happened before this table existed (see
# `benches/PROGRESS.md`, "the two apps disagreed about `small`").
#
# ## `rev` — the rebuild you trigger yourself
#
# Each tier carries a revision. It is a declared input of that tier's
# derivations *and* the `dataset_revision` its rows are filed under, so
# bumping the integer — and nothing else — rebuilds the seeds and makes every
# row at that tier a new row rather than a silent overwrite of the old one.
#
# It is the human override beside `gen.nix`'s source filtering: filtering
# stops unrelated edits from invalidating anything, `rev` is how you say "this
# dataset really did change" without waiting for a hash to notice.
let
  tiers = {
    # Correctness of the harness. Fills in seconds; measures nothing you
    # should quote.
    smoke = {
      rows = 1000;
      rev = 1;
    };
    # Today's declared size. Note that it is NOT what the pre-0065 corpus
    # holds: those rows were recorded at 100 000 (and one at 10), because
    # only the seeded app ever passed `--rows` and the runner's own default
    # was something else. That gap is the reason this table exists.
    small = {
      rows = 200000;
      rev = 1;
    };
    # The working size: past the page cache, still inside the cage's disk.
    large = {
      rows = 5000000;
      rev = 1;
    };
    # Deliberately exceeds RAM under the cage, which is the only way to
    # measure what an engine does when it cannot hold its working set.
    # Gated on the `large` fill's measured cost (RFC 0065 open question 1).
    huge = {
      rows = 50000000;
      rev = 1;
    };
  };
in
rec {
  inherit tiers;

  # What an unqualified run means. Changing it changes what `nix run .#bench`
  # measures, so it is a decision rather than a default.
  defaultTier = "small";

  benchSeed = 42;
  sd = toString benchSeed;

  # One tier, with the string forms every builder interpolates. A name with
  # no entry is a build error here rather than a mystery dataset later.
  forTier =
    name:
    let
      t = tiers.${name} or null;
    in
    if t == null then
      throw "bench: unknown tier ${name} (have: ${toString (builtins.attrNames tiers)})"
    else
      {
        inherit name;
        count = t.rows;
        rows = toString t.rows;
        rev = toString t.rev;
        # What derivations are named by: the count so a store path says what it
        # holds, the revision so bumping it builds a different path.
        tag = "${name}-${toString t.rows}-r${toString t.rev}";
      };

  selected = forTier defaultTier;

  # The pre-tier spelling, kept so every existing caller reads the table
  # instead of a loose number.
  rows = selected.rows;
}
