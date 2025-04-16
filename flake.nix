{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
      };
    };
  };
  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          overlays = [
            (import rust-overlay)
            (final: prev: {
              wasm-bindgen-cli = prev.wasm-bindgen-cli.overrideAttrs (old: rec {
                pname = "${old.pname}-fork";
                version = "fork";
                src = prev.fetchFromGitHub {
                  owner = "tomasol";
                  repo = "wasm-bindgen";
                  rev = "d501b68c5d459c41ba07d0b37bc1e84ae31cbfea";
                  sha256 = "sha256-Ny0Q2ul3s2bpA3itgOP5QuGIVldromGlX+pm0VcqSHc=";
                };
                nativeBuildInputs = with prev; [
                  cargo
                  rustc
                ];
                buildPhase = ''
                  cd crates/cli
                  cargo build
                '';
                installPhase = ''
                  mkdir -p $out/bin
                  cp ../../target/debug/wasm-bindgen $out/bin/
                '';

                doCheck = false;
              });
            })
          ];

          pkgs = import nixpkgs {
            inherit system overlays;
          };

          makeObelisk = buildType: customTarget:
            let
              cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
              version = cargoToml.workspace.package.version;

              cargoZigbuildWrapped = pkgs.writeShellScriptBin "cargo-zigbuild" ''
                #!${pkgs.runtimeShell}
                # Set cache directory within the sandbox's writable temp area
                # Using TMPDIR is standard practice for build-time caches
                export XDG_CACHE_HOME="''${TMPDIR:-/tmp}/.cache"
                # Ensure the base directory exists (optional, but good practice)
                mkdir -p "$XDG_CACHE_HOME"
                # Execute the real cargo-zigbuild, passing all arguments through
                exec ${pkgs.cargo-zigbuild}/bin/cargo-zigbuild "$@"
              '';

              commonArgs = {
                pname = "obelisk";
                inherit version;
                src = ./.;
                cargoLock = {
                  lockFile = ./Cargo.lock;
                  outputHashes = {
                    "getrandom-0.2.11" = "sha256-fBPB5ptPPBQqvsxTJd+LwKXBdChrVm75DQewyQUhM2Q=";
                  };
                };

                nativeBuildInputs = with pkgs; [
                  (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                  pkg-config
                  protobuf
                ] ++ pkgs.lib.optionals (customTarget != null) [
                  cargoZigbuildWrapped
                  pkgs.zig
                ];

                # Only used when not cross compiling.
                cargoBuildFlags = [ ];

                installPhase = ''
                  runHook preInstall
                  BINARY=$(find target -name obelisk)
                  mkdir -p $out/bin/
                  cp $BINARY $out/bin/
                  runHook postInstall
                '';

                doCheck = false;

                # Only used when not cross compiling.
                inherit buildType;
              };

              zigbuildArgs = pkgs.lib.optionalAttrs (customTarget != null) {
                # Override buildPhase only when using zigbuild
                buildPhase = ''
                  runHook preBuild

                  echo "Building with cargo zigbuild for target: ${customTarget}"
                  echo "Build type: ${buildType}"

                  # Construct flags
                  # Use --release flag only if buildType is "release"
                  RELEASE_FLAG=${pkgs.lib.optionalString (buildType == "release") "--release"}

                  # Call cargo zigbuild
                  cargo zigbuild \
                    $RELEASE_FLAG \
                    --locked \
                    --offline \
                    --target ${customTarget} \
                    ''${cargoBuildFlags} # Note the bash variable expansion syntax here

                  runHook postBuild
                '';
              };
            in
            pkgs.rustPlatform.buildRustPackage (commonArgs // zigbuildArgs);

        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                (pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                cargo-binstall
                cargo-edit
                cargo-expand
                cargo-generate
                cargo-insta
                cargo-nextest
                cargo-semver-checks
                cargo-zigbuild
                litecli
                nixd
                nixpkgs-fmt
                pkg-config
                protobuf
                release-plz
                wasm-tools
                wasmtime
                zig
              ];
          };
          devShells.web = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                (pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                binaryen # wasm-opt
                protobuf
                trunk
                wasm-bindgen-cli
              ];
          };
          devShells.publish = pkgs.mkShell {
            # Used to execute scripts/cargo-publish-workspace.sh
            buildInputs = [
              (pkgs.rust-bin.nightly."2025-04-01".default.override {
                extensions = [ "rust-src" "rustfmt" "clippy" ];
              })
            ];
          };

          packages = rec {
            obeliskLibcNix = makeObelisk "release" null;
            obeliskLibcNixDev = makeObelisk "dev" null;
            # x86_64
            obeliskCrossDev-x86_64-unknown-linux-musl = makeObelisk "dev" "x86_64-unknown-linux-musl";
            obeliskCross-x86_64-unknown-linux-musl = makeObelisk "release" "x86_64-unknown-linux-musl";
            obeliskCrossDev-x86_64-unknown-linux-gnu = makeObelisk "dev" "x86_64-unknown-linux-gnu.2.35";
            obeliskCross-x86_64-unknown-linux-gnu = makeObelisk "release" "x86_64-unknown-linux-gnu.2.35";
            # aarch64
            obeliskCrossDev-aarch64-unknown-linux-musl = makeObelisk "dev" "aarch64-unknown-linux-musl";
            obeliskCross-aarch64-unknown-linux-musl = makeObelisk "release" "aarch64-unknown-linux-musl";
            obeliskCrossDev-aarch64-unknown-linux-gnu = makeObelisk "dev" "aarch64-unknown-linux-gnu.2.35";
            obeliskCross-aarch64-unknown-linux-gnu = makeObelisk "release" "aarch64-unknown-linux-gnu.2.35";

            default = obeliskLibcNix;
          };
        }
      );
}
