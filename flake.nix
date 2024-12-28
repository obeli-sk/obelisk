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
                  if config == "x86_64-unknown-linux-musl" then {
                    inherit config;
                    rustc = { inherit config; };
                    isStatic = true;
                  } else null;
              };
          makeObelisk = pkgs: patch-for-generic-linux:

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
              buildType = "release";
              doCheck = false;
            };
          makeDocker = pkgs: obelisk: binSh:
            pkgs.dockerTools.buildImage {
              name = "obelisk";
              copyToRoot = pkgs.buildEnv {
                name = "image-root";
                paths = [ obelisk ] ++ (if binSh then [
                  pkgs.dockerTools.usrBinEnv
                  pkgs.dockerTools.binSh
                  pkgs.dockerTools.caCertificates
                ] else [ ]);
                pathsToLink = [ "/bin" ];
              };
              runAsRoot = ''
                #!${pkgs.runtimeShell}
                mkdir -p /data
              '';
              config = {
                Entrypoint = [ "/bin/obelisk" ];
                WorkingDir = "/data";
              };
            };
          pkgs = makePkgs null;
          pkgsMusl = makePkgs "x86_64-unknown-linux-musl";
        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                (pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml)
                cargo-binstall
                cargo-dist
                cargo-edit
                cargo-expand
                cargo-generate
                cargo-insta
                cargo-nextest
                # cargo-semver-checks
                dive
                git-cliff
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
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
          packages = rec {
            obelisk = makeObelisk pkgs false;
            obeliskPatchForGenericLinux = makeObelisk pkgs true;
            obeliskMusl = makeObelisk pkgsMusl false;
            dockerLibcForUbuntu = makeDocker pkgs obeliskPatchForGenericLinux false;
            default = obelisk;
          };
        }
      );
}
