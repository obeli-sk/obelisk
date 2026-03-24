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
                release-plz
                wasm-tools
                wasmtime.out
              ];
          };
          devShells.cargo-zigbuild = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                cargo-zigbuild
            ];
          };
          devShells.sandbox = pkgs.mkShell {
            packages = with pkgs; [
              codex
              gemini-cli
              claude-code
              bubblewrap
              # tools
              git
              curl
              helix
              wget
              htop
              zellij
              procps
              ripgrep
              which
              less
            ];
            shellHook = ''
              CURRENT_DIR=$(pwd)

              # SSL/Network Fixes
              export SSL_CERT_FILE="${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              export NIX_SSL_CERT_FILE=$SSL_CERT_FILE
              REAL_RESOLV=$(realpath /etc/resolv.conf)
              REAL_HOSTS=$(realpath /etc/hosts)
              REAL_GITCONFIG=$(realpath "$HOME/.gitconfig")

              # Construct mocked /run/current-system/sw/bin
              MOCKED_SYSTEM_BIN=$(mktemp -d)
              # Iterate through current PATH and symlink executables.
              # We use 'ln -s' without '-f' (force) so that the FIRST entry found
              # in the PATH (highest priority) wins, mimicking actual shell behavior.
              IFS=':' read -ra PATH_DIRS <<< "$PATH"
              for dir in "''${PATH_DIRS[@]}"; do
                if [ -d "$dir" ]; then
                   ln -s "$dir"/* "$MOCKED_SYSTEM_BIN/" 2>/dev/null || true
                fi
              done

              BWRAP_CMD=(
                ${pkgs.bubblewrap}/bin/bwrap
                --unshare-all
                --share-net
                --die-with-parent
                # --- Essential Binds ---
                --ro-bind /nix /nix
                --proc /proc
                --dev /dev
                --tmpfs /tmp
                # Tools need these to know "who" is running the process
                --ro-bind /etc/passwd /etc/passwd
                --ro-bind /etc/group /etc/group
                # --- Network ---
                --ro-bind "$REAL_RESOLV" /etc/resolv.conf
                --ro-bind "$REAL_HOSTS"  /etc/hosts
                # Git
                --ro-bind "$REAL_GITCONFIG" /tmp/.gitconfig
                # Claude
                --bind $HOME/.claude /tmp/.claude
                --bind $HOME/.claude.json /tmp/.claude.json
                # Cargo
                --bind $HOME/.cargo  /tmp/.cargo
                # --- Project Mount ---
                --dir /workspace
                --bind "$CURRENT_DIR" /workspace
                --chdir /workspace
                # --- Mocked System Bin ---
                # Create the directory structure in the sandbox
                --dir /run/current-system/sw/bin
                # Bind our constructed temp folder to it
                --ro-bind "$MOCKED_SYSTEM_BIN" /run/current-system/sw/bin
                --ro-bind "$MOCKED_SYSTEM_BIN" /usr/bin
                # --- Environment ---
                --setenv PS1 "[BWRAP] \w> "
                --setenv HOME /tmp
                --setenv TMPDIR /tmp
                --setenv TEMP /tmp
                --setenv CARGO_TARGET_DIR target-sandbox
              )
              exec "''${BWRAP_CMD[@]}" ${pkgs.bashInteractive}/bin/bash -l +m
            '';
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
