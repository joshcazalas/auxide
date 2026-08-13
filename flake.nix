{
  description = "Development, test, and package environment for discord-music-bot";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs =
    inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [ "x86_64-linux" ];

      perSystem =
        { pkgs, ... }:
        let
          runtimePath = pkgs.lib.makeBinPath [
            pkgs.deno
            pkgs.ffmpeg-headless
            pkgs.yt-dlp
          ];

          discord-music-bot = pkgs.rustPlatform.buildRustPackage {
            pname = "discord-music-bot";
            version = "0.1.0";
            src = pkgs.lib.cleanSource ./.;
            cargoLock.lockFile = ./Cargo.lock;

            nativeBuildInputs = [
              pkgs.makeWrapper
              pkgs.pkg-config
            ];
            buildInputs = [ pkgs.libopus ];

            postInstall = ''
              wrapProgram "$out/bin/discord-music-bot" \
                --prefix PATH : ${runtimePath}
            '';
          };
        in
        {
          packages = {
            default = discord-music-bot;
            inherit discord-music-bot;
          };

          checks = {
            inherit discord-music-bot;
          };

          devShells.default = pkgs.mkShell {
            packages = [
              pkgs.cargo
              pkgs.clippy
              pkgs.deno
              pkgs.ffmpeg-headless
              pkgs.libopus
              pkgs.pkg-config
              pkgs.rustc
              pkgs.rustfmt
              pkgs.yt-dlp
            ];

            RUST_BACKTRACE = "1";
          };
        };
    };
}
