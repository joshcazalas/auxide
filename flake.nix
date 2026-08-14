{
  description = "Auxide Discord music bot";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs =
    inputs@{
      self,
      flake-parts,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" ];

      flake.nixosModules.default = import ./nix/module.nix self;

      perSystem =
        { pkgs, ... }:
        let
          runtimePath = pkgs.lib.makeBinPath [
            pkgs.deno
            pkgs.ffmpeg-headless
            pkgs.yt-dlp
          ];

          auxide = pkgs.rustPlatform.buildRustPackage {
            pname = "auxide";
            version = "0.1.0";
            src = pkgs.lib.cleanSource ./.;
            cargoLock.lockFile = ./Cargo.lock;

            nativeBuildInputs = [
              pkgs.makeWrapper
              pkgs.pkg-config
            ];
            buildInputs = [ pkgs.libopus ];

            postInstall = ''
              wrapProgram "$out/bin/auxide" \
                --prefix PATH : ${runtimePath}
            '';
          };

          credential-helper = pkgs.writeShellApplication {
            name = "auxide-credential";
            runtimeInputs = [
              pkgs.coreutils
              pkgs.systemd
            ];
            text = builtins.readFile ./nix/auxide-credential.sh;
          };

          oci-image = pkgs.dockerTools.buildLayeredImage {
            name = "ghcr.io/joshcazalas/auxide";
            tag = "latest";
            contents = [
              auxide
              pkgs.cacert
            ];
            extraCommands = ''
              mkdir -p tmp run/auxide
              chmod 1777 tmp
              chmod 0755 run run/auxide
            '';
            config = {
              Entrypoint = [ "${auxide}/bin/auxide" ];
              Cmd = [
                "--config"
                "/run/auxide/config.toml"
                "run"
              ];
              Env = [ "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt" ];
              User = "65532:65532";
              WorkingDir = "/";
            };
          };
        in
        {
          packages = {
            default = auxide;
            inherit credential-helper auxide oci-image;
          };

          checks = {
            inherit credential-helper auxide oci-image;
          };

          devShells.default = pkgs.mkShell {
            packages = [
              pkgs.cargo
              pkgs.clippy
              pkgs.gitleaks
              pkgs.actionlint
              pkgs.deadnix
              pkgs.deno
              pkgs.ffmpeg-headless
              pkgs.libopus
              pkgs.pkg-config
              pkgs.rustc
              pkgs.rustfmt
              pkgs.shellcheck
              pkgs.statix
              pkgs.yt-dlp
            ];

            RUST_BACKTRACE = "1";
          };

          devShells.release = pkgs.mkShell {
            packages = [
              pkgs.gh
              pkgs.jq
              pkgs.sbomnix
              pkgs.skopeo
            ];
          };

          formatter = pkgs.nixfmt-tree;
        };
    };
}
