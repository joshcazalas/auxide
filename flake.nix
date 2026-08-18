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
          # Lets yt-dlp ask a running provider for a proof-of-origin token.
          #
          # Without one, YouTube hands over roughly the first megabyte of a
          # track and refuses the rest, which reaches a listener as a song that
          # stops a minute in. This half only knows how to ask; something has to
          # be listening on services.auxide.poTokenProviderUrl to answer, which
          # the NixOS module runs as a container beside the bot.
          #
          # Not in nixpkgs, so it is built from the published release here. The
          # wheel rather than the sdist because the 1.3.1 sdist is a malformed
          # tarball — it carries a `../README.md` that tar refuses to extract.
          bgutil-pot-plugin = pkgs.python3Packages.buildPythonPackage rec {
            pname = "bgutil-ytdlp-pot-provider";
            version = "1.3.1";
            format = "wheel";
            src = pkgs.fetchPypi {
              pname = "bgutil_ytdlp_pot_provider";
              inherit version format;
              dist = "py3";
              python = "py3";
              hash = "sha256-5ish+bLkR51Zr4eokAOHw0iS6Nf9siPyZnSakOC+It4=";
            };
            # A yt-dlp plugin rather than an importable library, so there is no
            # module of its own name to check for.
            pythonImportsCheck = [ ];
            doCheck = false;
          };

          # yt-dlp finds plugins on its own Python path, so the plugin is added
          # to the interpreter yt-dlp runs under rather than passed by flag.
          # That keeps the discovery a packaging concern and leaves the bot's
          # command line to say only what it wants, not where the code lives.
          yt-dlp-with-pot = pkgs.yt-dlp.overridePythonAttrs (previous: {
            dependencies = (previous.dependencies or [ ]) ++ [ bgutil-pot-plugin ];
          });

          runtimePath = pkgs.lib.makeBinPath [
            pkgs.deno
            pkgs.ffmpeg-headless
            yt-dlp-with-pot
          ];

          auxide = pkgs.rustPlatform.buildRustPackage {
            pname = "auxide";
            version = "0.1.0";
            src = pkgs.lib.cleanSource ./.;
            cargoLock.lockFile = ./Cargo.lock;

            # scripts/check.sh already runs this crate's tests. Leaving the
            # default check phase on rebuilt every test target a second time
            # under the release profile's codegen-units = 1 and thin LTO, which
            # spent three and a quarter minutes compiling in order to run
            # twenty-seven tests that finish in twenty-four milliseconds.
            doCheck = false;

            nativeBuildInputs = [
              pkgs.makeWrapper
              pkgs.pkg-config
            ];
            buildInputs = [ pkgs.libopus ];

            postInstall = ''
              wrapProgram "$out/bin/auxide" \
                --prefix PATH : ${runtimePath}
            '';

            meta = {
              description = "Self-hosted Discord music bot with encrypted voice";
              homepage = "https://github.com/joshcazalas/auxide";
              license = pkgs.lib.licenses.mit;
              mainProgram = "auxide";
              platforms = [ "x86_64-linux" ];
            };
          };

          enabledModule = inputs.nixpkgs.lib.nixosSystem {
            system = pkgs.stdenv.hostPlatform.system;
            modules = [
              self.nixosModules.default
              {
                services.auxide.enable = true;
                system.stateVersion = "26.05";
              }
            ];
          };

          module-evaluation = pkgs.runCommand "auxide-nixos-module-evaluation" { } ''
            test ${pkgs.lib.escapeShellArg enabledModule.config.systemd.services.auxide.serviceConfig.User} = auxide
            test ${pkgs.lib.escapeShellArg enabledModule.config.systemd.services.auxide.serviceConfig.Group} = auxide
            touch "$out"
          '';

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
            inherit
              credential-helper
              auxide
              module-evaluation
              oci-image
              ;
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
              # The same yt-dlp the bot runs, plugin and all. Without it the
              # daily probe cannot ask for a proof-of-origin token, so it would
              # be checking a configuration nothing ships.
              yt-dlp-with-pot
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
