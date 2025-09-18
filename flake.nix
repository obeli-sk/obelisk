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

          makeObelisk = buildType: customTarget: rustToolchainToml:
            let
              cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
              version = cargoToml.workspace.package.version;

              isMacOSTarget = (customTarget == "x86_64-apple-darwin" || customTarget == "aarch64-apple-darwin"); # FIXME: ends with
              macOSsdkTarball =
                if isMacOSTarget then
                  pkgs.fetchurl
                    {
                      name = "MacOSX11.3.sdk.tar.xz"; # Optional: better name for the store path
                      url = "https://github.com/phracker/MacOSX-SDKs/releases/download/11.3/MacOSX11.3.sdk.tar.xz";
                      # Confirmed hash for this URL
                      sha256 = "sha256-zU8Ip1V3FFuPBSRaKXX3yBQB116VNdz/u4ee4d7vy/Q=";
                    }
                else
                  null;

              manualMacOSSdk =
                if isMacOSTarget then
                  pkgs.stdenv.mkDerivation
                    {
                      pname = "unpacked-macosx-sdk";
                      version = "11.3";
                      # Use the fetched tarball as the source
                      src = macOSsdkTarball;
                      # Add tools needed for unpacking
                      nativeBuildInputs = [ pkgs.xz ];
                      dontConfigure = true;
                      dontBuild = true;
                      dontUnpack = true;

                      installPhase = ''
                        runHook preInstall

                        echo "Unpacking $src into $(pwd)..."
                        # Extract directly into the current directory
                        tar -xJf $src -v --strip-components=0 # Adjust strip-components if needed
                        local extractedDir="MacOSX11.3.sdk" # Assuming this is the top-level dir in the tarball
                        if [ ! -d "$extractedDir" ]; then
                          echo "Error: Directory '$extractedDir' not found after unpacking $src"
                          echo "Contents of current directory:"
                          ls -la
                          exit 1
                        fi
                        echo "Creating $out/SDKs and moving $extractedDir into it"
                        mkdir -p $out/SDKs
                        # Move the extracted directory to the final location
                        mv "$extractedDir" "$out/SDKs/MacOSX11.3.sdk"
                        # Check if the final SDK directory exists
                        if [ ! -d "$out/SDKs/MacOSX11.3.sdk" ]; then
                          echo "Error: Failed to find SDK directory in $out/SDKs after moving."
                          exit 1
                        fi
                        echo "Successfully installed SDK to $out/SDKs/MacOSX11.3.sdk"

                        runHook postInstall
                      '';
                    }
                else
                  null; # No unpacked SDK if not targeting macOS

              cargoZigbuildWrapped = pkgs.writeShellScriptBin "cargo-zigbuild" ''
                #!${pkgs.runtimeShell}
                # Set cache directory within the sandbox's writable temp area
                export XDG_CACHE_HOME="''${TMPDIR:-/tmp}/.cache"
                mkdir -p "$XDG_CACHE_HOME"
                ${pkgs.lib.optionalString isMacOSTarget ''
                export SDKROOT="${manualMacOSSdk}/SDKs/MacOSX11.3.sdk"
                echo "Setting SDKROOT for macOS cross-compilation: $SDKROOT"
                if [ ! -d "$SDKROOT" ]; then
                  echo "Error: SDKROOT directory does not exist!"
                  exit 1
                fi
                ''}
                export CARGO_ZIGBUILD_ZIG_PATH="${pkgs.zig_0_13}/bin/zig"
                echo "Setting zig to $CARGO_ZIGBUILD_ZIG_PATH"
                exec ${pkgs.cargo-zigbuild}/bin/cargo-zigbuild "$@"
              '';

              commonArgs = {
                pname = "obelisk";
                inherit version;
                src = ./.;
                cargoLock = {
                  lockFile = ./Cargo.lock;
                };

                nativeBuildInputs = with pkgs; [
                  (rust-bin.fromRustupToolchainFile rustToolchainToml)
                  pkg-config
                  protobuf
                  findutils # for installPhase
                ]
                # Add Zig when cross compiling
                ++ pkgs.lib.optionals (customTarget != null) [
                  cargoZigbuildWrapped
                  pkgs.zig_0_13
                ]
                # Add macOS SDK only if targeting macOS
                ++ pkgs.lib.optionals isMacOSTarget [
                  manualMacOSSdk
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

                  # Use --release flag only if buildType is "release"
                  RELEASE_FLAG=${pkgs.lib.optionalString (buildType == "release") "--release"}

                  # Call cargo zigbuild
                  cargo zigbuild \
                    $RELEASE_FLAG \
                    --locked \
                    --offline \
                    --target ${customTarget} \
                    --verbose \
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
                (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                cargo-binstall
                cargo-edit
                cargo-expand
                cargo-generate
                cargo-insta
                cargo-nextest
                cargo-semver-checks
                cargo-zigbuild # only here for dev-deps
                litecli
                nixd
                nixpkgs-fmt
                pkg-config
                protobuf
                release-plz
                wasm-tools
                wasmtime.out
              ];
          };
          devShells.web = pkgs.mkShell {
            # Contains the forked wasm-bindgen-cli
            nativeBuildInputs = with pkgs;
              [
                (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                binaryen # wasm-opt
                protobuf
                trunk
                wasm-bindgen-cli
              ];
          };

          packages = rec {
            obeliskLibcNixDev = makeObelisk "dev" null ./rust-toolchain.toml;
            obeliskLibcNix = makeObelisk "release" null ./rust-toolchain.toml;
            # Linux
            ## x86_64
            obeliskCrossDev-x86_64-unknown-linux-musl = makeObelisk "dev" "x86_64-unknown-linux-musl" ./rust-toolchain-cross.toml;
            obeliskCross-x86_64-unknown-linux-musl = makeObelisk "release" "x86_64-unknown-linux-musl" ./rust-toolchain-cross.toml;
            obeliskCrossDev-x86_64-unknown-linux-gnu = makeObelisk "dev" "x86_64-unknown-linux-gnu.2.35" ./rust-toolchain-cross.toml;
            obeliskCross-x86_64-unknown-linux-gnu = makeObelisk "release" "x86_64-unknown-linux-gnu.2.35" ./rust-toolchain-cross.toml;
            ## aarch64
            obeliskCrossDev-aarch64-unknown-linux-musl = makeObelisk "dev" "aarch64-unknown-linux-musl" ./rust-toolchain-cross.toml;
            obeliskCross-aarch64-unknown-linux-musl = makeObelisk "release" "aarch64-unknown-linux-musl" ./rust-toolchain-cross.toml;
            obeliskCrossDev-aarch64-unknown-linux-gnu = makeObelisk "dev" "aarch64-unknown-linux-gnu.2.35" ./rust-toolchain-cross.toml;
            obeliskCross-aarch64-unknown-linux-gnu = makeObelisk "release" "aarch64-unknown-linux-gnu.2.35" ./rust-toolchain-cross.toml;
            # MacOS
            ## x86_64
            obeliskCrossDev-x86_64-apple-darwin = makeObelisk "dev" "x86_64-apple-darwin" ./rust-toolchain-cross.toml;
            obeliskCross-x86_64-apple-darwin = makeObelisk "release" "x86_64-apple-darwin" ./rust-toolchain-cross.toml;
            ## aarch64
            obeliskCrossDev-aarch64-apple-darwin = makeObelisk "dev" "aarch64-apple-darwin" ./rust-toolchain-cross.toml;
            obeliskCross-aarch64-apple-darwin = makeObelisk "release" "aarch64-apple-darwin" ./rust-toolchain-cross.toml;

            default = obeliskLibcNix;
          };
        }
      );
}
