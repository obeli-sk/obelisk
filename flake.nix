{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
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
          makeObelisk = pkgs:
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
              nativeBuildInputs = [ pkgs.pkg-config ];
              PKG_CONFIG_PATH = "${pkgs.openssl.dev}/lib/pkgconfig";
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
                owner = "tomasol";
                repo = "wasm-pkg-tools";
                rev = "355769f3b08e430d2b7c3c1a9a0b43d19fdb56cd";
                hash = "sha256-a/5foBFjRBU0pd/wth2ytchleDfLB/gEBQzfaFszWEk=";
              };
              cargoHash = "sha256-IxdZ742h8BSKWhxt7W2HzvWITfanmwX6NXpPfvGjeo8=";
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
                rustToolchain
                tokio-console
                wasm-tools
                wkg
              ];
          };

          packages = rec {
            obelisk = makeObelisk pkgs;
            obeliskMusl = makeObelisk pkgsMusl;
            docker = makeDocker pkgs obeliskMusl false;
            dockerBinSh = makeDocker pkgs obeliskMusl true;
            default = obelisk;
          };
        }
      );
}
