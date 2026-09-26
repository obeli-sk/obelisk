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

          rustyV8Version = "150.4.0";
          rustyV8NativeTarget = {
            x86_64-linux = "x86_64-unknown-linux-gnu";
            aarch64-linux = "aarch64-unknown-linux-gnu";
            x86_64-darwin = "x86_64-apple-darwin";
            aarch64-darwin = "aarch64-apple-darwin";
          }.${system};
          rustyV8Hashes = {
            x86_64-unknown-linux-gnu = { archive = "sha256-9IdiyhDR8fxgWkQcWuQw7Izh6egPFNePvELLh4wwtHY="; binding = "sha256-dyeCauR5vbZF6Acjn7EtH44uI956bPFvXuWSaQ0dhQY="; };
            aarch64-unknown-linux-gnu = { archive = "sha256-U54oOBWjlqV5bzKFi0LlF7hY66rqqtBdAykO6MhkpSc="; binding = "sha256-dyeCauR5vbZF6Acjn7EtH44uI956bPFvXuWSaQ0dhQY="; };
            x86_64-unknown-linux-musl = { archive = "sha256-QGuqbhoa/Wi8ehOqp59yuAZ0a4i0ug1VR56mgwMryv0="; binding = "sha256-dyeCauR5vbZF6Acjn7EtH44uI956bPFvXuWSaQ0dhQY="; };
            aarch64-unknown-linux-musl = { archive = "sha256-VNtqagjqHWWPybQGyOf4dqYlQ6WqkRHzqYtYm11ja/Y="; binding = "sha256-dyeCauR5vbZF6Acjn7EtH44uI956bPFvXuWSaQ0dhQY="; };
            x86_64-apple-darwin = { archive = "sha256-p1AnH+xrIRRX7Qpc99LqsZJLJlYhqC2oarlZ1v8II+Q="; binding = "sha256-ylrfDPicmnCtRgrnNkiy/om3SqETs8t/dXtqArdYOU8="; };
            aarch64-apple-darwin = { archive = "sha256-Wu/9jVoMG3msHXCvg9WxkJllX9nGRaeU3EPxAfd5g4w="; binding = "sha256-ylrfDPicmnCtRgrnNkiy/om3SqETs8t/dXtqArdYOU8="; };
          };

          # Fixed-output derivations that pre-fetch the pinned operator-owned WASM assets
          # referenced from `crates/embedded-assets/*version*.txt`, so the embedding build.rs
          # can read them offline inside the Nix sandbox. Update the hashes whenever a version
          # file changes with `scripts/update-embedded-assets-hash.sh`.
          fetchOciAssets = { pname, outputHash, assets }:
            pkgs.stdenv.mkDerivation {
              inherit pname outputHash;
              version = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
              nativeBuildInputs = with pkgs; [ skopeo jq cacert ];
              outputHashMode = "recursive";
              outputHashAlgo = "sha256";
              SSL_CERT_FILE = "${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt";
              buildCommand = ''
                mkdir -p "$out"
                export HOME="$TMPDIR"
                # Avoid the containers/image "/run/containers/<uid>/auth.json" fallback,
                # which can exist but be unreadable on CI runners (permission denied).
                export XDG_RUNTIME_DIR="$TMPDIR"
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
                ${pkgs.lib.concatMapStringsSep "\n" (a: "fetch ${a.name} ${./crates/embedded-assets + "/${a.versionFile}"}") assets}
              '';
            };
          embeddedWebui = fetchOciAssets {
            pname = "obelisk-embedded-webui";
            outputHash = "sha256-C1vWXAU9UC0znl3b/WFA0XpW05ClKvSva6VHAkfc8/Y=";
            assets = [
              { name = "webui"; versionFile = "webui-version.txt"; }
            ];
          };
          embeddedJsRuntimes = fetchOciAssets {
            pname = "obelisk-embedded-js-runtimes";
            outputHash = "sha256-UQJsW2eJjX6lm50jI5T8ZHnHO2c9PqL7Zv7qiMu2PeQ=";
            assets = [
              { name = "activity"; versionFile = "activity-js-runtime-version.txt"; }
              { name = "workflow"; versionFile = "workflow-js-runtime-version.txt"; }
              { name = "webhook"; versionFile = "webhook-js-runtime-version.txt"; }
            ];
          };

          # `embedJsRuntimes = false` still embeds the web UI; JS runtimes are then fetched at runtime.
          makeObelisk = buildType: customTarget: rustToolchainToml: embedJsRuntimes:
            let
              cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
              version = cargoToml.workspace.package.version;
              rustyV8Target = if customTarget == null then rustyV8NativeTarget else builtins.replaceStrings [ ".2.35" ] [ "" ] customTarget;
              rustyV8Mirror =
                let
                  targets = pkgs.lib.unique [ rustyV8NativeTarget rustyV8Target ];
                  links = target:
                    let
                      hashes = rustyV8Hashes.${target};
                      baseUrl = "https://github.com/denoland/rusty_v8/releases/download/v${rustyV8Version}";
                      archiveName = "librusty_v8_simdutf_release_${target}.a.gz";
                      bindingName = "src_binding_simdutf_release_${target}.rs";
                      archive = pkgs.fetchurl { url = "${baseUrl}/${archiveName}"; hash = hashes.archive; };
                      binding = pkgs.fetchurl { url = "${baseUrl}/${bindingName}"; hash = hashes.binding; };
                    in
                    ''
                      ln -s ${archive} "$out/v${rustyV8Version}/${archiveName}"
                      ln -s ${binding} "$out/v${rustyV8Version}/${bindingName}"
                    '';
                in
                pkgs.runCommand "rusty-v8-mirror" { } ''
                  mkdir -p "$out/v${rustyV8Version}"
                  ${pkgs.lib.concatMapStringsSep "\n" links targets}
                '';

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

                cargoBuildFlags = pkgs.lib.optionals embedJsRuntimes [ "--features" "embed-js-runtimes" ];

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
              # Feed the pre-fetched assets to the embedding build.rs offline.
              embedAssetsEnv = {
                OBELISK_EMBED_ASSETS_DIR = pkgs.symlinkJoin {
                  name = "obelisk-embedded-assets";
                  paths = [ embeddedWebui ] ++ pkgs.lib.optionals embedJsRuntimes [ embeddedJsRuntimes ];
                };
              };
              rustyV8MirrorEnv = { RUSTY_V8_MIRROR = "${rustyV8Mirror}"; };
            in
            pkgs.rustPlatform.buildRustPackage (commonArgs // zigbuildArgs // embedAssetsEnv // rustyV8MirrorEnv);

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
                gh # scripts/sync-branch-protection.sh
                git-cliff
                jq
                litecli
                nixd
                nixpkgs-fmt
                pkg-config
                postgresql
                protobuf
                sqlite
                wasm-tools
                yq-go # scripts/sync-branch-protection.sh
              ];
          };
          # Builds the native WASIp3 fixtures published by push-test-components.
          # Keep this separate from the stable toolchain used for Obelisk releases.
          devShells.wasip3-components = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                (rust-bin.nightly."2026-09-12".default.override {
                  targets = [ "wasm32-unknown-unknown" "wasm32-wasip2" "wasm32-wasip3" ];
                })
                pkg-config
                protobuf
                wasm-tools
              ];
          };
          devShells.cargo-zigbuild = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                cargo-zigbuild
              ];
          };
          packages = rec {
            inherit embeddedWebui embeddedJsRuntimes;
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
