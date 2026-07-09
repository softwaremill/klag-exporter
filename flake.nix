{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs:
    inputs.flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ (import inputs.rust-overlay) ];
        };
        inherit (pkgs) lib;
        cargoToml = lib.fromTOML (lib.readFile ./Cargo.toml);

        rustToolchain = pkgs.rust-bin.stable."1.96.0".default.override {
          extensions = [
            "clippy"
            "rust-analyzer"
            "rust-src"
            "rustfmt"
          ];
        };

        klag-exporter =
          (pkgs.makeRustPlatform {
            cargo = rustToolchain;
            rustc = rustToolchain;
          }).buildRustPackage
            {
              pname = cargoToml.package.name;
              version = "${cargoToml.package.version}-${
                if lib.hasAttr "revCount" inputs.self then
                  "${lib.toString inputs.self.revCount}-${inputs.self.shortRev}"
                else
                  "gitDirty"
              }";

              src = lib.cleanSource ./.;
              cargoLock.lockFile = ./Cargo.lock;
              LIBCLANG_PATH = "${lib.makeLibraryPath [ pkgs.libclang ]}";

              buildInputs = [
                pkgs.openssl
                pkgs.cyrus_sasl
                pkgs.curl
              ];
              nativeBuildInputs = [
                pkgs.pkg-config
                pkgs.cmake
              ];
            };
      in
      {
        checks = { inherit klag-exporter; };
        devShells.default = pkgs.mkShell {
          hardeningDisable = [ "fortify" ];
          LIBCLANG_PATH = "${lib.makeLibraryPath [ pkgs.libclang ]}"; # for dev builds
          inputsFrom = [ klag-exporter ];
          packages = with pkgs; [
            cargo-watch
          ];
        };
        packages = {
          inherit klag-exporter;
          default = klag-exporter;
        };
      }
    );
}
