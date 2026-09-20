# The run entry points.
#
# `benches/` is outside the cargo workspace on purpose (the drivers must not
# reach the shipped dependency graph), so these build it in place against the
# checkout rather than from the store.
#
# The app runs the **supervisor** (`bench`), uncaged. It resolves which rows
# this pass needs, then execs one `bench-row` per row inside its own cage
# (RFC 0065 §2) — so the wrapper's job is now to declare the budgets and put
# the peers on `PATH`, not to wrap the work.
#
# One pair of apps **per tier** (`bench-large`, `bench-seeded-large`, …), plus
# the unsuffixed pair for the default. The suffix is not decoration: the tier
# decides what is measured *and* what the row is filed as, so an app that took
# it as a flag would be an app you can point at the wrong dataset.
{
  pkgs,
  cage,
  runtimeInputs,
  benchSeeds,
  tier,
  # Appended to both app names. Empty for the default tier, so
  # `nix run .#bench` keeps working.
  suffix ? "",
}:
let
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
            ${cage.exports}
            ${
              pkgs.lib.concatMapStrings (s: s + "\n") (
                pkgs.lib.mapAttrsToList (k: v: ''export ${k}="${v}"'') exports
              )
            }# Both binaries: the supervisor locates `bench-row` beside itself,
            # so they must be built from the same tree in the same place.
            cargo build --release --manifest-path "$repo/benches/Cargo.toml" \
              --bin bench --bin bench-row
            # The tier, its row count and its revision come from ONE place
            # (`params.nix`), and every app passes all three. Passing the name
            # without the count is how a 100 000-row run once filed itself as
            # `small`: the name is what the corpus keys on, the count is what
            # was actually measured, and nothing but this line ties them.
            exec "$repo/benches/target/release/bench" \
              --repo "$repo" \
              --results "$repo/benches/results" \
              --cage-revision ${cage.revision} \
              --tier ${tier.name} \
              --rows ${tier.rows} \
              --dataset-revision ${tier.rev} \
              ${toString (args ++ [ ''"$@"'' ])}
          '';
        }
      }/bin/${name}";
    };
in
{
  # Fill from empty, in-run, at the selected tier. The seeds are not inputs
  # here, so this builds nothing but the runner — the shape to reach for when
  # what you are changing is the fill itself.
  "bench${suffix}" = runner { name = "bench${suffix}"; };

  # Materialise every seed and run against them, so a repeat run skips the
  # fill entirely. Building this app builds all five seeds, which is the
  # point: they are inputs, not a side effect.
  "bench-seeded${suffix}" = runner {
    name = "bench-seeded${suffix}";
    exports = {
      BENCH_SEED_WAVEDB = "${benchSeeds.wavedb}";
      BENCH_SEED_SQLITE = "${benchSeeds.sqlite}";
      BENCH_SEED_POSTGRES = "${benchSeeds.postgres}";
      BENCH_SEED_MYSQL = "${benchSeeds.mysql}";
      BENCH_SEED_MONGODB = "${benchSeeds.mongodb}";
    };
  };
}
