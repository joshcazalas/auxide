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
in
{
  options.services.auxide = {
    enable = lib.mkEnableOption "Auxide Discord music bot";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.system}.auxide;
      defaultText = lib.literalExpression "auxide.packages.${pkgs.system}.auxide";
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

    environment.systemPackages = [ self.packages.${pkgs.system}.credential-helper ];

    systemd.tmpfiles.rules = [
      "d /var/lib/auxide 0700 root root -"
    ];

    systemd.services.auxide = {
      description = "Auxide Discord music bot";
      documentation = [ "https://github.com/joshcazalas/auxide" ];
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];

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
