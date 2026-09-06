# The two run entry points.
#
# `benches/` is outside the cargo workspace on purpose (the drivers must not
# reach the shipped dependency graph), so both build it in place against the
# checkout rather than from the store.
{
  pkgs,
  cage,
  runtimeInputs,
  benchSeeds,
  rows,
}:
let
  # Everything the two share: locate the checkout, point `pkg-config` at the
  # pinned SQLite, build the runner, then exec it caged.
  #
  #   exports  extra environment a variant needs, as `NAME=value` pairs;
  #   args     extra runner flags, as tokens.
  #
  # Both are lists rather than strings so an empty one contributes nothing at
  # all — no stray blank line, no doubled separator in the command.
  runner =
    {
      name,
      exports ? { },
      args ? [ ],
    }:
    {
      type = "app";
      program = "${
        pkgs.writeShellApplication {
          inherit name runtimeInputs;
          text = ''
            set -euo pipefail
            repo="$(git rev-parse --show-toplevel)"
            export PKG_CONFIG_PATH="${pkgs.sqlite.dev}/lib/pkgconfig"
            ${
              pkgs.lib.concatMapStrings (s: s + "\n") (
                pkgs.lib.mapAttrsToList (k: v: ''export ${k}="${v}"'') exports
              )
            }cargo build --release --manifest-path "$repo/benches/Cargo.toml"
            ${cage.wrap} "$repo/benches/target/release/wavedb-bench" \
              ${toString ([ ''--repo "$repo"'' ] ++ args ++ [ ''"$@"'' ])}
          '';
        }
      }/bin/${name}";
    };
in
{
  # Fill from empty, in-run. The seeds are not inputs here, so this builds
  # nothing but the runner — the shape to reach for when what you are changing
  # is the fill itself.
  bench = runner { name = "bench"; };

  # Materialise every seed and run against them, so a repeat run skips the
  # fill entirely. Building this app builds all five seeds, which is the
  # point: they are inputs, not a side effect.
  bench-seeded = runner {
    name = "bench-seeded";
    exports = {
      BENCH_SEED_WAVEDB = "${benchSeeds.wavedb}";
      BENCH_SEED_SQLITE = "${benchSeeds.sqlite}";
      BENCH_SEED_POSTGRES = "${benchSeeds.postgres}";
      BENCH_SEED_MYSQL = "${benchSeeds.mysql}";
      BENCH_SEED_MONGODB = "${benchSeeds.mongodb}";
    };
    args = [ "--rows ${rows}" ];
  };
}
