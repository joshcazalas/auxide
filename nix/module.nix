self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.auxide;
  credentialsDirectory = "/run/credentials/auxide.service";
  system = pkgs.stdenv.hostPlatform.system;
  # Named by the backend actually in use, so this keeps working if a host is
  # configured for podman rather than docker.
  providerUnit = "${config.virtualisation.oci-containers.backend}-auxide-pot-provider.service";
in
{
  options.services.auxide = {
    enable = lib.mkEnableOption "Auxide Discord music bot";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${system}.auxide;
      defaultText = lib.literalExpression "auxide.packages.${system}.auxide";
      description = "Auxide package to run.";
    };

    configFile = lib.mkOption {
      type = lib.types.path;
      default = "/var/lib/auxide/config.toml";
      description = ''
        Root-owned, server-local Auxide configuration. This file is copied into the service's
        protected systemd credential directory at startup and is never written by NixOS activation.
      '';
    };

    credentialFile = lib.mkOption {
      type = lib.types.path;
      default = "/var/lib/auxide/discord-token";
      description = ''
        Host-encrypted Discord token created by auxide-credential. Never point this option at
        plaintext data or a path in the Nix store.
      '';
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "auxide";
      description = "Unprivileged system account used by Auxide.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "auxide";
      description = "Primary group used by Auxide.";
    };

    extraGroups = lib.mkOption {
      type = lib.types.listOf lib.types.str;
      default = [ ];
      description = "Supplementary groups, for example a read-only media-library group.";
    };

    memoryMax = lib.mkOption {
      type = lib.types.str;
      default = "1G";
      description = "Hard systemd memory limit for Auxide and its media subprocesses.";
    };

    poTokenProvider = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = ''
          Run a GVS proof-of-origin token provider beside Auxide.

          YouTube serves roughly the first megabyte of a track to a client that cannot present
          one of these and refuses the rest, which reaches a listener as a song that stops about
          a minute in rather than as an error. Turning this off is only sensible when a provider
          is already running elsewhere, in which case point `youtube.po_token_base_url` in the
          Auxide configuration at it.
        '';
      };

      port = lib.mkOption {
        type = lib.types.port;
        default = 4416;
        description = ''
          Loopback port the provider listens on. Never published beyond 127.0.0.1: it issues
          tokens on request and has no authentication of its own.
        '';
      };

      image = lib.mkOption {
        type = lib.types.str;
        default = "docker.io/brainicism/bgutil-ytdlp-pot-provider:1.3.1";
        description = ''
          Provider image, pinned by tag so an upstream rebuild cannot change what runs here
          without the change being visible in this file.
        '';
      };
    };
  };

  config = lib.mkIf cfg.enable {
    assertions = [
      {
        assertion = !lib.hasPrefix "/nix/store/" (toString cfg.configFile);
        message = "Auxide runtime configuration must remain outside the Nix store.";
      }
      {
        assertion = !lib.hasPrefix "/nix/store/" (toString cfg.credentialFile);
        message = "Auxide's Discord token must remain outside the Nix store.";
      }
    ];

    users.groups.${cfg.group} = { };
    users.users.${cfg.user} = {
      isSystemUser = true;
      inherit (cfg) extraGroups group;
      home = "/var/empty";
      createHome = false;
    };

    # The administrative subcommands in docs/operator-guide.md are run by hand
    # against the same binary the service uses, so it has to be reachable by
    # name. Only the unit's ExecStart resolved it before, leaving `auxide` off
    # the operator's PATH entirely.
    environment.systemPackages = [
      cfg.package
      self.packages.${system}.credential-helper
    ];

    systemd.tmpfiles.rules = [
      "d /var/lib/auxide 0700 root root -"
    ];

    # Bound to loopback rather than published: the provider answers every
    # request that reaches it, so what reaches it is the only control there is.
    virtualisation.oci-containers.containers = lib.mkIf cfg.poTokenProvider.enable {
      auxide-pot-provider = {
        inherit (cfg.poTokenProvider) image;
        ports = [ "127.0.0.1:${toString cfg.poTokenProvider.port}:4416" ];
        extraOptions = [ "--init" ];
      };
    };

    systemd.services.auxide = {
      description = "Auxide Discord music bot";
      documentation = [ "https://github.com/joshcazalas/auxide" ];
      wantedBy = [ "multi-user.target" ];
      wants = [
        "network-online.target"
      ]
      ++ lib.optional cfg.poTokenProvider.enable providerUnit;
      # Wanted rather than required, and only ordered after: a provider that is
      # slow to start or has fallen over should cost whole tracks, not the bot.
      # Auxide answers commands, reports what YouTube said, and recovers on its
      # own when the provider comes back.
      after = [
        "network-online.target"
      ]
      ++ lib.optional cfg.poTokenProvider.enable providerUnit;

      serviceConfig = {
        Type = "simple";
        ExecStart = "${lib.getExe cfg.package} --config ${credentialsDirectory}/auxide-config run";
        User = cfg.user;
        Group = cfg.group;
        LoadCredential = [ "auxide-config:${toString cfg.configFile}" ];
        LoadCredentialEncrypted = [ "discord-token:${toString cfg.credentialFile}" ];

        Restart = "on-failure";
        RestartSec = "5s";
        TimeoutStopSec = "30s";
        UMask = "0077";

        CacheDirectory = "auxide";
        RuntimeDirectory = "auxide";
        CacheDirectoryMode = "0700";
        RuntimeDirectoryMode = "0700";

        CapabilityBoundingSet = "";
        DevicePolicy = "closed";
        LockPersonality = true;
        MemoryMax = cfg.memoryMax;
        NoNewPrivileges = true;
        PrivateDevices = true;
        PrivateTmp = true;
        ProcSubset = "pid";
        ProtectClock = true;
        ProtectControlGroups = true;
        ProtectHome = true;
        ProtectHostname = true;
        ProtectKernelLogs = true;
        ProtectKernelModules = true;
        ProtectKernelTunables = true;
        ProtectProc = "invisible";
        ProtectSystem = "strict";
        RemoveIPC = true;
        RestrictAddressFamilies = [
          "AF_INET"
          "AF_INET6"
          "AF_UNIX"
        ];
        RestrictNamespaces = true;
        RestrictRealtime = true;
        RestrictSUIDSGID = true;
        SystemCallArchitectures = "native";
        TasksMax = 128;
      };
    };
  };
}
