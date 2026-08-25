{
  nixConfig = {
    extra-substituters = [
      "https://obeli-sk.cachix.org"
    ];
    extra-trusted-public-keys = [
      "obeli-sk.cachix.org-1:31iM9GWSEhAXvvuTWQ7CvAcwvgRzsuJ9yJghywSd3Jw="
    ];
  };

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
          ];

          pkgs = import nixpkgs {
            inherit system overlays;
          };

          # Fixed-output derivation that pre-fetches the four pinned operator-owned WASM
          # assets (three JS runtimes + web UI) referenced from `crates/embedded-assets/*version*.txt`,
          # so the `embed-assets` build.rs can read them offline inside the Nix sandbox.
          # Update `outputHash` whenever any version file changes with
          # `scripts/update-embedded-assets-hash.sh`.
          embeddedAssets =
            let
              assetsDir = ./crates/embedded-assets;
            in
            pkgs.stdenv.mkDerivation {
              pname = "obelisk-embedded-assets";
              version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
              nativeBuildInputs = with pkgs; [ skopeo jq cacert ];
              outputHashMode = "recursive";
              outputHashAlgo = "sha256";
              outputHash = "sha256-X+xrRrbYLnhXiWFM7tmuvBrPZLCv4guZRY92eD3+WQI=";
              SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
              buildCommand = ''
                mkdir -p "$out"
                export HOME="$TMPDIR"
                fetch() {
                  name="$1"; versionFile="$2"
                  # Strip the `oci://` scheme and drop the `:tag` (skopeo rejects tag+digest;
                  # the `@sha256:` digest is the authoritative pin).
                  ref=$(tr -d '[:space:]' < "$versionFile" | sed 's|^oci://||' | sed -E 's|:[^:@]*@sha256:|@sha256:|')
                  echo "Fetching $name from $ref"
                  skopeo --insecure-policy copy "docker://$ref" "dir:$TMPDIR/$name"
                  layer=$(jq -r '.layers[0].digest' "$TMPDIR/$name/manifest.json" | sed 's|^sha256:||')
                  cp "$TMPDIR/$name/$layer" "$out/$name.wasm"
                }
                fetch activity ${assetsDir}/activity-js-runtime-version.txt
                fetch workflow ${assetsDir}/workflow-js-runtime-version.txt
                fetch webhook  ${assetsDir}/webhook-js-runtime-version.txt
                fetch webui    ${assetsDir}/webui-version.txt
              '';
            };

          makeObelisk = buildType: customTarget: rustToolchainToml: embedAssets:
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

                # Also spliced into the cross (zigbuild) build command below.
                cargoBuildFlags = pkgs.lib.optionals embedAssets [ "--features" "embed-assets" ];

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

              zigbuildArgs = pkgs.lib.optionalAttrs (customTarget != null)
                {
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
                } // pkgs.lib.optionalAttrs (customTarget == "x86_64-unknown-linux-gnu.2.35") {
                # Native-architecture Zig probes need Nix's loader inside the build sandbox.
                CFLAGS_x86_64_unknown_linux_gnu = "-Wl,--dynamic-linker=${pkgs.stdenv.cc.bintools.dynamicLinker}";
              };
              # Feed the pre-fetched assets to the `embed-assets` build.rs offline.
              embedAssetsEnv = pkgs.lib.optionalAttrs embedAssets {
                OBELISK_EMBED_ASSETS_DIR = "${embeddedAssets}";
              };
            in
            pkgs.rustPlatform.buildRustPackage (commonArgs // zigbuildArgs // embedAssetsEnv);

        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                actionlint
                (rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                cargo-audit
                cargo-deny
                cargo-edit
                cargo-expand
                cargo-insta
                cargo-nextest
                jq
                litecli
                nixd
                nixpkgs-fmt
                pkg-config
                protobuf
                git-cliff
                wasm-tools
              ];
          };
          devShells.cargo-zigbuild = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                cargo-zigbuild
              ];
          };
          # Used only by release-2-cargo-publish to get a cargo with the
          # workspace-publish fix (rust-lang/cargo#17071), which is missing
          # from the 1.96.0 stable toolchain pinned in rust-toolchain.toml.
          # TODO: remove this devshell once rust-toolchain.toml is on >= 1.98
          # (the first stable with the fix) and revert release-2 to the
          # default `nix develop`.
          devShells.publish = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                (rust-bin.beta.latest.default.override {
                  targets = [ "wasm32-unknown-unknown" "wasm32-wasip2" ];
                })
                pkg-config
                protobuf
              ];
          };
          packages = rec {
            inherit embeddedAssets;
            obeliskLibcNixDev = makeObelisk "dev" null ./rust-toolchain.toml false;
            obeliskLibcNix = makeObelisk "release" null ./rust-toolchain.toml false;
            obeliskLibcNixDev-embedded = makeObelisk "dev" null ./rust-toolchain.toml true;
            obeliskLibcNix-embedded = makeObelisk "release" null ./rust-toolchain.toml true;
            # Linux
            ## x86_64
            obeliskCross-x86_64-unknown-linux-musl = makeObelisk "release" "x86_64-unknown-linux-musl" ./rust-toolchain-cross.toml false;
            obeliskCross-x86_64-unknown-linux-musl-embedded = makeObelisk "release" "x86_64-unknown-linux-musl" ./rust-toolchain-cross.toml true;
            obeliskCross-x86_64-unknown-linux-gnu = makeObelisk "release" "x86_64-unknown-linux-gnu.2.35" ./rust-toolchain-cross.toml false;
            obeliskCross-x86_64-unknown-linux-gnu-embedded = makeObelisk "release" "x86_64-unknown-linux-gnu.2.35" ./rust-toolchain-cross.toml true;
            ## aarch64
            obeliskCross-aarch64-unknown-linux-musl = makeObelisk "release" "aarch64-unknown-linux-musl" ./rust-toolchain-cross.toml false;
            obeliskCross-aarch64-unknown-linux-musl-embedded = makeObelisk "release" "aarch64-unknown-linux-musl" ./rust-toolchain-cross.toml true;
            obeliskCross-aarch64-unknown-linux-gnu = makeObelisk "release" "aarch64-unknown-linux-gnu.2.35" ./rust-toolchain-cross.toml false;
            obeliskCross-aarch64-unknown-linux-gnu-embedded = makeObelisk "release" "aarch64-unknown-linux-gnu.2.35" ./rust-toolchain-cross.toml true;
            # MacOS
            ## x86_64
            obeliskCross-x86_64-apple-darwin = makeObelisk "release" "x86_64-apple-darwin" ./rust-toolchain-cross.toml false;
            obeliskCross-x86_64-apple-darwin-embedded = makeObelisk "release" "x86_64-apple-darwin" ./rust-toolchain-cross.toml true;
            ## aarch64
            obeliskCross-aarch64-apple-darwin = makeObelisk "release" "aarch64-apple-darwin" ./rust-toolchain-cross.toml false;
            obeliskCross-aarch64-apple-darwin-embedded = makeObelisk "release" "aarch64-apple-darwin" ./rust-toolchain-cross.toml true;

            default = obeliskLibcNix;
          };
        }
      );
}
