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
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          rustToolchain = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              cargo-component
              cargo-dist
              cargo-expand
              cargo-insta
              cargo-nextest
              litecli
              lldb
              nixpkgs-fmt
              pkg-config
              rustToolchain
              tokio-console
              wasm-tools
            ];
          };

          packages = {
            default = pkgs.rustPlatform.buildRustPackage {
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
            };
            # default = packages.app;
          };
        }
      );
}
