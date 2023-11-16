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
            nativeBuildInputs = with pkgs; [ rustToolchain pkg-config wasm-tools nixpkgs-fmt rustup ];
            buildInputs = with pkgs; [ openssl ];
            shellHook = ''
              project_root="$(git rev-parse --show-toplevel 2>/dev/null)"
              export CARGO_INSTALL_ROOT="$project_root/.cargo"
              export PATH="$CARGO_INSTALL_ROOT/bin:$PATH"
              cargo_packages="cargo-component@0.4.1"
              cargo install --offline $cargo_packages 2>/dev/null || cargo install $cargo_packages
            '';
          };
        }
      );
}
