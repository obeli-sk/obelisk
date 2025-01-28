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
          overlays = [ (import rust-overlay) ];
          makePkgs = config:
            import nixpkgs
              {
                inherit system overlays;
                crossSystem =
                if config != null then { # the parameter is not null only when building -musl targets.
                    inherit config;
                    rustc = { inherit config; };
                    isStatic = true;
                    } else null;
              };
          makeObelisk = pkgs: patch-for-generic-linux: buildType:
            pkgs.rustPlatform.buildRustPackage {
              pname = "obelisk";
              version = "0.0.1";
              src = ./.;
              cargoLock = {
                lockFile = ./Cargo.lock;
                outputHashes = {
                  "getrandom-0.2.11" = "sha256-fBPB5ptPPBQqvsxTJd+LwKXBdChrVm75DQewyQUhM2Q=";
                };
              };
              nativeBuildInputs = with pkgs; [
                (pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                patchelf
                pkg-config
                protobuf
              ];
              installPhase = ''
                BINARY=$(find target -name obelisk)
                ${if patch-for-generic-linux then ''patchelf --set-interpreter /lib64/ld-linux-x86-64.so.2 $BINARY''
                else '''' }
                mkdir -p $out/bin/
                cp $BINARY $out/bin/
              '';
              inherit buildType;
              doCheck = false;
            };
          pkgs = makePkgs null;
          pkgsMusl = if system == "x86_64-linux" then makePkgs "x86_64-unknown-linux-musl"
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
                lldb
                nixd
                nixpkgs-fmt
                pkg-config
                protobuf
                release-plz
                tokio-console
                wasm-tools
                wasmtime
                # webui
                binaryen
                tailwindcss
                trunk
                wasm-bindgen-cli
              ];
          };
          packages = rec {
            obeliskGlibcNix = makeObelisk pkgs false "release";
            obeliskGlibcNixDev = makeObelisk pkgs false "dev";
            obeliskGlibcGeneric = makeObelisk pkgs true "release";
            obeliskGlibcGenericDev = makeObelisk pkgs true "dev";
            default = obeliskGlibcNix;
          } // (if pkgsMusl != null then {
            obeliskMusl = makeObelisk pkgsMusl false "release";
            obeliskMuslDev = makeObelisk pkgsMusl false "dev";
          } else {});
        }
      );
}
