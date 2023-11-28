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
                version = "0.5.0";
                src = prev.fetchFromGitHub { 
                      owner = "bytecodealliance";
                      repo = "cargo-component";
                      rev = "v0.5.0";
                      sha256 = "sha256-P7gXfACPK63f38KzV6UVQa8MZmxEaMNxl1GZYCDM54M=";
                };
                cargoDeps = old.cargoDeps.overrideAttrs (pkgs.lib.const {
                  inherit src;
                  outputHash = "sha256-GDQbzuOcP2Ce5MuKvsiarzFeaF+AtiZ2Vp2h1GMZMR0=";
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
              nixpkgs-fmt
              pkg-config
              rustToolchain
              wasm-tools
            ];
          };
        }
      );
}
