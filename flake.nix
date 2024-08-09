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
          rustToolchain = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
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
          wkg =
            pkgs.rustPlatform.buildRustPackage {
              pname = "wkg";
              version = "0.3.0";
              src = pkgs.fetchFromGitHub {
                owner = "bytecodealliance";
                repo = "wasm-pkg-tools";
                rev = "ff5e16297453930435fdf4126d50c6ec99c0a909";
                hash = "sha256-cBxXV9eGMU/sB6eNWs0GwVQEtEYrGxm+29OAW75vtgA=";
              };
              cargoHash = "sha256-biCexpnRlYdO6VXfseYmAjave430JN1JJB9n12B2RXo=";
              nativeBuildInputs = [ pkgs.pkg-config ];
              PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
              doCheck = false;
            };

          pkgsMusl = makePkgs "x86_64-unknown-linux-musl";
        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs;
              [
                cargo-binstall
                cargo-component
                cargo-dist
                cargo-expand
                cargo-insta
                cargo-nextest
                dive
                litecli
                lldb
                nixpkgs-fmt
                pkg-config
                protobuf
                rustToolchain
                tokio-console
                wasm-tools
              ];
            PROTOC = "${pkgs.protobuf}/bin/protoc";
          };
          packages = rec {
            inherit wkg;
            obelisk = makeObelisk pkgs false;
            obelisk-patch-for-generic-linux = makeObelisk pkgs true;
            obeliskMusl = makeObelisk pkgsMusl false;
            docker = makeDocker pkgs obeliskMusl false;
            dockerBinSh = makeDocker pkgs obeliskMusl true;
            default = obelisk;
          };

        }
      );
}
