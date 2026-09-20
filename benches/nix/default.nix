# ── bench: the comparative benchmark suite (RFCs 0060, 0065) ─────────────────
#
# Both brackets: the WaveDB engine in-process against SQLite, and MongoDB /
# PostgreSQL / MySQL over a local connection.
#
# This is the wiring only — every part lives in a sibling module:
#
#   params.nix    the tier table (rows + revision) and the seed
#   cage.nix      the cgroup/affinity/namespace every measured run executes in
#   gen.nix       `bench-gen`, the fill/emit tool (filtered source)
#   dataset.nix   the portable TSV every bulk loader reads
#   seeds.nix     the five pre-filled data directories
#   runtime.nix   what a run needs on PATH
#   apps.nix      the run entry points
#
# ## Every tier is built, one is default
#
# The dataset and the five seeds are a function of the tier, so they are
# instantiated once per tier and named for it: `bench-seed-sqlite-large`,
# `bench-dataset-smoke`. The unsuffixed names are the default tier, which is
# what `nix run .#bench` measures.
#
# Instantiating all four costs nothing — evaluation is lazy, so `huge`'s
# derivations exist as expressions and are built only if something asks. What
# it buys is that a tier cannot be run without its dataset existing under a
# name that says which tier it is.
#
# `flake.nix` imports this and re-exports `packages`/`apps` from it.
{
  pkgs,
  rustPlatform,
  rustToolchain,
  repoSrc,
}:
let
  inherit (pkgs) lib;
  params = import ./params.nix;
  inherit (params) sd fillArgs;

  cage = import ./cage.nix;

  benchGen = import ./gen.nix { inherit pkgs rustPlatform repoSrc; };

  runtimeInputs = import ./runtime.nix { inherit pkgs rustToolchain; };

  # One tier's whole world: its dataset, its five seeds, its two apps.
  mkTier =
    name:
    let
      tier = params.forTier name;
      benchDataset = import ./dataset.nix {
        inherit
          pkgs
          benchGen
          tier
          sd
          ;
      };
      benchSeeds = import ./seeds.nix {
        inherit
          pkgs
          benchGen
          benchDataset
          tier
          sd
          fillArgs
          ;
      };
    in
    {
      inherit tier benchDataset benchSeeds;
      apps =
        suffix:
        import ./apps.nix {
          inherit
            pkgs
            cage
            runtimeInputs
            benchSeeds
            tier
            suffix
            ;
        };
    };

  built = lib.mapAttrs (name: _: mkTier name) params.tiers;
  default = built.${params.defaultTier};

  # `bench-dataset-large`, `bench-seed-sqlite-large`, …
  packagesFor =
    name: t:
    {
      "bench-dataset-${name}" = t.benchDataset;
    }
    // lib.mapAttrs' (system: drv: lib.nameValuePair "bench-seed-${system}-${name}" drv) t.benchSeeds;
in
{
  # `nix build .#bench-seed-postgres` etc. Keep the result symlinks as GC
  # roots, or a `nix-collect-garbage` between runs eats the fill.
  packages = {
    bench-gen = benchGen;

    # The default tier, unsuffixed — the names every existing script uses.
    bench-dataset = default.benchDataset;
    bench-seed-wavedb = default.benchSeeds.wavedb;
    bench-seed-sqlite = default.benchSeeds.sqlite;
    bench-seed-postgres = default.benchSeeds.postgres;
    bench-seed-mysql = default.benchSeeds.mysql;
    bench-seed-mongodb = default.benchSeeds.mongodb;
  }
  // lib.concatMapAttrs packagesFor built;

  apps = default.apps "" // lib.concatMapAttrs (name: t: t.apps "-${name}") built;
}
