# `bench-gen` — the fill/emit tool every seed is built with.
#
# ## Why the source is filtered
#
# `bench-gen` is a build input of **every seed**, so whatever invalidates it
# invalidates the whole dataset tree. Built from `repoSrc` — the flake's own
# source, the entire checkout — that meant an RFC edit, a README fix, or
# **recording a benchmark row** rebuilt all five seeds. The last one is the
# sharp edge: `benches/results/` is tracked, so storing a measurement
# invalidated the datasets the next measurement needs. At the `small` tier
# that is minutes; at `large` it is the afternoon.
#
# So the source is narrowed to what this binary actually compiles from: the
# bench crate's Rust and manifests, the WaveDB crates it links, and the
# workspace root manifest those crates inherit from. Nothing else can reach a
# `.rs` file, so nothing else should be able to change a store path.
#
# The pairing with `params.nix`'s per-tier `rev` is deliberate: filtering
# stops unrelated edits from invalidating anything, and `rev` is how a human
# says "this dataset really did change" without waiting for a hash to notice.
{
  pkgs,
  rustPlatform,
  repoSrc,
}:
let
  inherit (pkgs) lib;
  inherit (lib.fileset) unions fileFilter toSource;

  rust = dir: fileFilter (f: f.hasExt "rs") dir;
  manifests = dir: fileFilter (f: f.name == "Cargo.toml") dir;
in
rustPlatform.buildRustPackage {
  pname = "bench-gen";
  version = "0.1.0";

  src = toSource {
    root = repoSrc;
    fileset = unions [
      # The bench crate: its sources, its manifest, and the lock that makes
      # this build hermetic.
      (rust (repoSrc + "/benches/src"))
      (repoSrc + "/benches/Cargo.toml")
      (repoSrc + "/benches/Cargo.lock")
      # The engine, by path dependency.
      (rust (repoSrc + "/crates"))
      (manifests (repoSrc + "/crates"))
      # Every crate manifest says `workspace = true` somewhere, so cargo
      # walks up to this one. Without it the path deps do not resolve.
      (repoSrc + "/Cargo.toml")
    ];
  };

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
