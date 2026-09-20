# ── bench: the comparative benchmark suite (RFC 0060) ────────────────────────
#
# Both brackets: the WaveDB engine in-process against SQLite, and MongoDB /
# PostgreSQL / MySQL over a local connection.
#
# This is the wiring only — every part lives in a sibling module:
#
#   params.nix    the tier table (rows + revision) and the seed
#   cage.nix      the cgroup/affinity/namespace every measured run executes in
#   gen.nix       `bench-gen`, the fill/emit tool
#   dataset.nix   the portable TSV every bulk loader reads
#   seeds.nix     the five pre-filled data directories
#   runtime.nix   what a run needs on PATH
#   apps.nix      the two run entry points
#
# `flake.nix` imports this and re-exports `packages`/`apps` from it.
{
  pkgs,
  rustPlatform,
  rustToolchain,
  repoSrc,
}:
let
  params = import ./params.nix;
  inherit (params) sd;
  # One tier for the whole file: the seeds, the dataset and both apps must
  # describe the same dataset or a row is filed under a name it was not
  # measured against.
  tier = params.selected;

  cage = import ./cage.nix;

  benchGen = import ./gen.nix { inherit pkgs rustPlatform repoSrc; };

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
      ;
  };

  runtimeInputs = import ./runtime.nix { inherit pkgs rustToolchain; };
in
{
  # `nix build .#bench-seed-postgres` etc. Keep the result symlinks as GC
  # roots, or a `nix-collect-garbage` between runs eats the fill.
  packages = {
    bench-gen = benchGen;
    bench-dataset = benchDataset;
    bench-seed-wavedb = benchSeeds.wavedb;
    bench-seed-sqlite = benchSeeds.sqlite;
    bench-seed-postgres = benchSeeds.postgres;
    bench-seed-mysql = benchSeeds.mysql;
    bench-seed-mongodb = benchSeeds.mongodb;
  };

  apps = import ./apps.nix {
    inherit
      pkgs
      cage
      runtimeInputs
      benchSeeds
      tier
      ;
  };
}
