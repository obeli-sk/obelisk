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
          overlays = [ (import rust-overlay) (final: prev: {
              cargo-component = prev.cargo-component.overrideAttrs (old: rec {
                version = "0.6.0";
                src = prev.fetchFromGitHub {
                      owner = "tomasol";
                      repo = "cargo-component";
                      rev = "c9ac8531f8ecfb1df07a0ce48fa6ec241f9eb630";
                      sha256 = "sha256-B50wIMKuyYjxPLRUH1pekgcpAs7LI+MR/heDihehEnU=";
                };
                cargoDeps = old.cargoDeps.overrideAttrs (pkgs.lib.const {
                  inherit src;
                  outputHash = "sha256-sNdjatP22TSxWz8mJN91sAjx6nVOAf6hZYMPEeRRHe0=";
                });
              });
            })
          ];
          pkgs = import nixpkgs {
            inherit system overlays;
          };
          rustToolchain = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        in
        {
          devShells.default = pkgs.mkShell {
            nativeBuildInputs = with pkgs; [
              cargo-component
              cargo-expand
              cargo-nextest
              nixpkgs-fmt
              pkg-config
              rustToolchain
              wasm-tools
            ];
          };
        }
      );
}
