# `bench-gen` — the fill/emit tool every seed is built with.
#
# Built once, from the repo source, so a schema change invalidates it and
# therefore every seed downstream. That automatic invalidation is the whole
# reason the seeds are Nix derivations and not a `~/.cache` directory.
{
  pkgs,
  rustPlatform,
  repoSrc,
}:
rustPlatform.buildRustPackage {
  pname = "bench-gen";
  version = "0.1.0";
  src = repoSrc;
  # The bench crate is outside the workspace, so it carries its own lock —
  # which is what makes this build hermetic.
  cargoLock.lockFile = ../Cargo.lock;
  # Both are needed: `cargoRoot` says where the lock to vendor from lives (the
  # root one is the workspace's, a different dependency set),
  # `buildAndTestSubdir` says what to build.
  cargoRoot = "benches";
  buildAndTestSubdir = "benches";
  cargoBuildFlags = [
    "--bin"
    "bench-gen"
  ];
  # Without the `servers` feature. `bench-gen` fills WaveDB and writes a TSV;
  # it needs no database client, and it is a build input of every seed, so
  # compiling three drivers here would be paid on every seed rebuild.
  buildNoDefaultFeatures = true;
  doCheck = false;
  nativeBuildInputs = [ pkgs.pkg-config ];
  # rusqlite links the pinned system SQLite, never its bundled copy.
  buildInputs = [ pkgs.sqlite ];
}
