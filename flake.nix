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
                      rev = "898209357f582b89c52633d47ea7b16b9ddbc203";
                      sha256 = "sha256-GtLQXNzkFcFt9ciVxvDAxaGJSZNSahvKZpfBnkvanu4=";
                };
                cargoDeps = old.cargoDeps.overrideAttrs (pkgs.lib.const {
                  inherit src;
                  outputHash = "sha256-sqLSNhmpYpMZhX4QIr6wRB5wJCXBglme1jvyyPtzb5U=";
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
