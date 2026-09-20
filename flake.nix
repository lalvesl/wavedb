{
  description = "WaveDB — user-partitioned, tenant-centric embedded database";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        # ── shared modules (nix/) ─────────────────────────────────────────────
        pkgs = import ./nix/pkgs.nix { inherit nixpkgs system rust-overlay; };

        inherit
          (import ./nix/rust.nix {
            inherit pkgs;
            toolchainFile = ./rust-toolchain.toml;
          })
          rustToolchain
          rustPlatform
          ;

        guiLibs = import ./nix/gui-libs.nix pkgs;

        # ── the benchmark suite (benches/nix/) ────────────────────────────────
        bench = import ./benches/nix {
          inherit pkgs rustPlatform rustToolchain;
          repoSrc = ./.;
        };

        # wasm-bindgen-cli built at the exact version used by the crate in Cargo.lock.
        wasmBindgenCli = pkgs.rustPlatform.buildRustPackage rec {
          pname = "wasm-bindgen-cli";
          version = "0.2.121";

          src = pkgs.fetchCrate {
            inherit pname version;
            hash = "sha256-ZOMgFNOcGkO66Jz/Z83eoIu+DIzo3Z/vq6Z5g6BDY/w=";
          };

          cargoHash = "sha256-DPdCDPTAPBrbqLUqnCwQu1dePs9lGg85JCJOCIr9qjU=";

          nativeBuildInputs = [ pkgs.pkg-config ];
          buildInputs = [
            pkgs.openssl
          ]
          ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.darwin.apple_sdk.frameworks.Security
          ];
        };
      in
      {
        # `bench.packages` carries the default tier under the plain names and
        # every tier under a suffixed one (`bench-seed-sqlite-large`); the
        # tier list lives in `benches/nix/params.nix`, so it is merged rather
        # than enumerated here — a second copy of that list is a second place
        # to forget a tier.
        packages = bench.packages // {
          wasm = rustPlatform.buildRustPackage {
            pname = "wavedb-wasm";
            version = "0.1.0";
            src = ./.;

            cargoLock.lockFile = ./Cargo.lock;

            nativeBuildInputs = [
              wasmBindgenCli
              pkgs.binaryen # wasm-opt
              pkgs.gzip
            ];

            # The exported `example_roundtrip` entry point (src/example.rs)
            # exercises the engine — schema macro, migration chain, query DSL,
            # IndexedDB — so fat LTO keeps the codebase and the size below is a
            # meaningful number, not an empty shell.
            buildPhase = ''
              runHook preBuild
              cargo build --target wasm32-unknown-unknown --profile wasm-release -p wavedb-wasm
              runHook postBuild
            '';

            installPhase = ''
              runHook preInstall
              mkdir -p $out
              wasm-bindgen \
                --out-dir $out \
                --target bundler \
                target/wasm32-unknown-unknown/wasm-release/wavedb_wasm.wasm

              # Post-link size pass.  Feature flags match what rustc 1.8x+
              # emits and wasm-bindgen's externref pass requires.
              for f in $out/*_bg.wasm; do
                before=$(stat -c%s "$f")
                wasm-opt -Oz \
                  --enable-bulk-memory \
                  --enable-sign-ext \
                  --enable-mutable-globals \
                  --enable-nontrapping-float-to-int \
                  --enable-reference-types \
                  "$f" -o "$f.opt"
                mv "$f.opt" "$f"
                after=$(stat -c%s "$f")
                gzipped=$(gzip -9 -c "$f" | wc -c)
                echo "wasm size: $f  raw=$after (was $before)  gzip=$gzipped"
              done
              runHook postInstall
            '';

            doCheck = false;
          };
        };

        devShells.default = pkgs.mkShell {
          nativeBuildInputs = with pkgs; [
            pkg-config
            rustToolchain

            # Code quality
            cargo-mutants
            cargo-deny
            taplo
            nixpkgs-fmt
            prettier

            # Testing
            cargo-nextest

            # WASM
            wasm-pack
            wasm-bindgen-cli
          ];

          buildInputs =
            with pkgs;
            [
              openssl
              # The benchmark's SQLite peer (RFC 0060) links the system
              # library, never `rusqlite`'s bundled copy — the version has to
              # come from `flake.lock` like every other measured system.
              sqlite
            ]
            ++ lib.optionals stdenv.isLinux guiLibs
            ++ lib.optionals stdenv.isDarwin [
              darwin.apple_sdk.frameworks.SystemConfiguration
              darwin.apple_sdk.frameworks.CoreFoundation
              darwin.apple_sdk.frameworks.Security
            ];

          shellHook = ''
            export PKG_CONFIG_PATH="${pkgs.openssl.dev}/lib/pkgconfig:${pkgs.sqlite.dev}/lib/pkgconfig"
            ${pkgs.lib.optionalString pkgs.stdenv.isLinux ''
              export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath guiLibs}:$LD_LIBRARY_PATH"
            ''}
          '';
        };

        # ── bench (benches/nix) ───────────────────────────────────────────────
        #
        # The packages are merged into `packages` above. The apps are merged
        # here for the same reason: `bench` and `bench-seeded` are the default
        # tier, and each tier adds its own pair (`bench-large`,
        # `bench-seeded-large`). `nix flake show` lists them all.

        apps = bench.apps // {
          wavedb_monitor = {
            type = "app";
            program = "${
              pkgs.writeShellApplication {
                name = "wavedb-monitor";
                runtimeInputs = [ rustToolchain ];
                text = ''
                  cargo run --release --bin wavedb-monitor "$@"
                '';
              }
            }/bin/wavedb-monitor";
          };

          # ── real_example: multi-process orchestrated load test ──────────────────
          #
          # Builds all five binaries first, then runs the orchestrator which
          # spawns subprocesses pointing at the sibling binaries in the same
          # release output directory.
          real_example = {
            type = "app";
            program = "${
              pkgs.writeShellApplication {
                name = "real_example";
                runtimeInputs = [ rustToolchain ];
                text = ''
                  set -euo pipefail

                  echo "── Building all real_example binaries ──────────────────────────"
                  cargo build --release \
                    --bin real_example \
                    --bin re_slow_node \
                    --bin re_quick_node \
                    --bin re_client \
                    --bin re_monitor

                  echo "── Launching orchestrator ──────────────────────────────────────"
                  # The orchestrator discovers its sibling binaries via
                  # std::env::current_exe() — all five binaries live in the same
                  # target/release/ directory after the cargo build above.
                  exec ./target/release/real_example "$@"
                '';
              }
            }/bin/real_example";
          };

          # ── real_example_gui: the load test, monitored by the desktop GUI ───────
          #
          # Same 500-client payment-gateway scenario as real_example, but the
          # monitor is the egui desktop GUI opened on the Data tab — watch the
          # record graph, throughput, and page maps move live under load. Close
          # the GUI window to stop the scenario.
          #
          #   nix run .#real_example_gui
          real_example_gui = {
            type = "app";
            program = "${
              pkgs.writeShellApplication {
                name = "real_example_gui";
                runtimeInputs = [
                  rustToolchain
                  pkgs.coreutils
                ];
                text = ''
                  set -euo pipefail

                  echo "── Building real_example + GUI binaries ─────────────────────────"
                  cargo build --release \
                    --bin real_example \
                    --bin re_slow_node \
                    --bin re_quick_node \
                    --bin re_client \
                    --bin wavedb-monitor-gui

                  echo "── Launching orchestrator with the GUI monitor ──────────────────"
                  # eframe links wayland/libGL/etc. dynamically — put them on the
                  # loader path for the GUI child the orchestrator spawns.
                  export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath guiLibs}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
                  export WAVE_MONITOR=gui
                  exec ./target/release/real_example "$@"
                '';
              }
            }/bin/real_example_gui";
          };

          # ── monitor_gui_demo: turnkey GUI monitor against a live cluster ─────────
          #
          # One command: build the binaries, start a keyed 3-node cluster in a
          # temp dir, seed three tenants, then open the GUI pointed at them with
          # the cluster key. Closing the GUI window tears the whole thing down
          # (the EXIT trap kills the nodes and removes the temp dir).
          #
          #   nix run .#monitor_gui_demo
          #   nix run .#monitor_gui_demo -- --tab data   # extra GUI flags pass through
          #
          # Uses fixed ports 7700/7701/7800 — stop any other cluster on those
          # ports first.
          monitor_gui_demo = {
            type = "app";
            program = "${
              pkgs.writeShellApplication {
                name = "monitor_gui_demo";
                runtimeInputs = [
                  rustToolchain
                  pkgs.coreutils
                ];
                text = ''
                  set -euo pipefail

                  # Demo cluster secret (32 bytes / 64 hex). Node-to-node + the
                  # monitor's HMAC tokens use this; clients write without it.
                  KEY=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f

                  echo "── Building demo binaries (release) ──────────────────────────────"
                  cargo build --release \
                    --bin wavedb-slow-node \
                    --bin wavedb-quick-node \
                    --bin wavedb-monitor-gui \
                    --bin re_client

                  bin=./target/release
                  data="$(mktemp -d /tmp/wavedb-gui-demo.XXXXXX)"
                  pids=()

                  cleanup() {
                    echo
                    echo "── Stopping demo cluster ───────────────────────────────────────"
                    for pid in "''${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
                    rm -rf "$data"
                  }
                  trap cleanup EXIT INT TERM

                  echo "── Starting keyed cluster (2 quick + 1 slow) in $data ──"
                  "$bin/wavedb-slow-node" --listen 127.0.0.1:7800 \
                    --data-dir "$data/slow" --cluster-key "$KEY" \
                    >"$data/slow.log" 2>&1 &
                  pids+=("$!")
                  sleep 1
                  "$bin/wavedb-quick-node" --listen 127.0.0.1:7700 \
                    --peers 127.0.0.1:7701 --slow-node 127.0.0.1:7800 \
                    --data-dir "$data/q0" --cluster-key "$KEY" \
                    >"$data/q0.log" 2>&1 &
                  pids+=("$!")
                  "$bin/wavedb-quick-node" --listen 127.0.0.1:7701 \
                    --peers 127.0.0.1:7700 --slow-node 127.0.0.1:7800 \
                    --data-dir "$data/q1" --cluster-key "$KEY" \
                    >"$data/q1.log" 2>&1 &
                  pids+=("$!")
                  sleep 2

                  echo "── Seeding tenants 42, 77, 1001 (writes over WebSocket) ──"
                  for tenant in 42 77 1001; do
                    WAVE_QN_WS_URLS="ws://127.0.0.1:7700/ws,ws://127.0.0.1:7701/ws" \
                    WAVE_TENANT="$tenant" WAVE_CLIENT_ID=0 WAVE_NUM_CLIENTS=1 \
                      timeout 3 "$bin/re_client" >/dev/null 2>&1 || true
                  done

                  echo "── Waiting for the first history flush to the slow node ──"
                  sleep 6

                  echo "── Launching GUI — close the window to stop the demo ──"
                  export LD_LIBRARY_PATH="${pkgs.lib.makeLibraryPath guiLibs}''${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
                  "$bin/wavedb-monitor-gui" \
                    --quick-nodes http://127.0.0.1:7700,http://127.0.0.1:7701 \
                    --slow-nodes http://127.0.0.1:7800 \
                    --cluster-key "$KEY" "$@"
                '';
              }
            }/bin/monitor_gui_demo";
          };

          fmt = {
            type = "app";
            program = "${
              pkgs.writeShellApplication {
                name = "fmt";
                runtimeInputs = with pkgs; [
                  rustToolchain
                  nixfmt
                  taplo
                  prettier
                  jq
                ];
                text = ''
                  nixfmt .
                  cargo fmt --all
                  taplo fmt
                  prettier --write "**/*.md"
                  while IFS= read -r -d "" f; do
                    tmp="$(mktemp)"
                    jq . "$f" > "$tmp" && mv "$tmp" "$f"
                  done < <(find . -name "*.jsonl" -not -path "./.git/*" -not -path "./target/*" -print0)
                '';
              }
            }/bin/fmt";
          };
        };
      }
    );
}
