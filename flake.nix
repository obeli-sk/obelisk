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
          makePkgs = config:
            import nixpkgs
              {
                inherit system overlays;
                crossSystem =
                  if config != null then {
                    # the parameter is not null only when building -musl targets.
                    inherit config;
                    rustc = { inherit config; };
                    isStatic = true;
                  } else null;
              };
          makeObelisk = pkgs: buildType:
            let
              cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
              version = cargoToml.workspace.package.version;
            in
            pkgs.rustPlatform.buildRustPackage {
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
                (pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                pkg-config
                protobuf
              ];
              installPhase =
                ''
                  BINARY=$(find target -name obelisk)
                  mkdir -p $out/bin/
                  cp $BINARY $out/bin/
                '';
              inherit buildType;
              doCheck = false;
            };
          pkgs = makePkgs null;
          pkgsMusl =
            if system == "x86_64-linux" then makePkgs "x86_64-unknown-linux-musl"
            else if system == "aarch64-linux" then makePkgs "aarch64-unknown-linux-musl"
            else null;
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
                litecli
                nixd
                nixpkgs-fmt
                pkg-config
                protobuf
                release-plz
                wasm-tools
                wasmtime
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
            buildInputs = [
              (pkgs.rust-bin.nightly."2025-04-01".default.override {
                extensions = [ "rust-src" "rustfmt" "clippy" ];
              })
            ];
          };
          devShells.release = pkgs.mkShell {
            buildInputs = with pkgs;[
              (pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
              protobuf
              cargo-zigbuild
              zig
            ];
          };
          packages = rec {
            obeliskLibcNix = makeObelisk pkgs "release";
            obeliskLibcNixDev = makeObelisk pkgs "dev";
            default = obeliskLibcNix;
          } // (if pkgsMusl != null then {
            # linux only
            obeliskMusl = makeObelisk pkgsMusl "release";
            obeliskMuslDev = makeObelisk pkgsMusl "dev";
          } else { });
        }
      );
}
