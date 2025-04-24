{
  inputs = {
    fenix = {
      url = "github:nix-community/fenix/monthly";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    { self, fenix, flake-utils, nixpkgs, naersk, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
    let
      toolchain = with fenix.packages.${system};
        combine [
          stable.cargo
          stable.clippy
          stable.rust-src
          stable.rustc
          stable.rustfmt
          stable.rust-analyzer
        ];

      overlays = [ (import rust-overlay) ];
      pkgs = import nixpkgs {
        inherit system overlays;
      };

      naersk' = pkgs.callPackage naersk { cargo = toolchain; rustc = toolchain; };
    in
    {
      packages.default = [
        naersk'.buildPackage
        {
          name = "bacon";
          src = pkgs.fetchFromGitHub {
            owner = "Canop";
            repo = "bacon";
          };
        }
      ];

      devShells.default = pkgs.mkShell {
        buildInputs = [
          toolchain
          pkgs.libiconv
          pkgs.cargo-audit
          pkgs.bacon
        ];
        packages = (with pkgs.nodePackages; [ pnpm ]);
        env = {
          RUST_LOG = "debug";
        };
      };
    });
}
