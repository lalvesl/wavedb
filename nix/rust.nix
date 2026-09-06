# The project toolchain, and the `rustPlatform` built on top of it.
#
# Everything that compiles Rust in this repo — the wasm artifact, the bench
# generator, every dev shell — goes through here, so no output can ever be
# built against a toolchain other than the pinned one.
{
  pkgs,
  toolchainFile,
}:
let
  # Reads channel, components, and targets from rust-toolchain.toml, so the
  # wasm32 target comes along without being restated.
  rustToolchain = pkgs.rust-bin.fromRustupToolchainFile toolchainFile;
in
{
  inherit rustToolchain;

  rustPlatform = pkgs.makeRustPlatform {
    cargo = rustToolchain;
    rustc = rustToolchain;
  };
}
