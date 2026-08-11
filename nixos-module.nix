{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.codex-api;
in
{
  options.services.codex-api = {
    enable = lib.mkEnableOption "the codex-api relay";

    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "inputs.codex-api.packages.${pkgs.stdenv.hostPlatform.system}.default";
      description = "The codex-api package to use.";
    };

    configFile = lib.mkOption {
      type = lib.types.nonEmptyStr;
      default = "/etc/codex-api/config.toml";
      description = ''
        Runtime path to the TOML configuration file. Use a path under /run for
        configuration supplied by agenix or sops-nix so secrets do not enter
        the Nix store.
      '';
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "codex-api";
      description = "User account under which codex-api runs.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "codex-api";
      description = "Group under which codex-api runs.";
    };
  };

  config = lib.mkIf cfg.enable {
    users.users = lib.mkIf (cfg.user == "codex-api") {
      codex-api = {
        isSystemUser = true;
        group = cfg.group;
      };
    };
    users.groups = lib.mkIf (cfg.group == "codex-api") { codex-api = { }; };

    environment.systemPackages = [ cfg.package ];
    environment.sessionVariables.CODEX_API_CONFIG = cfg.configFile;

    systemd.services.codex-api = {
      description = "ChatGPT Codex API relay";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      environment.CODEX_API_CONFIG = cfg.configFile;
      serviceConfig = {
        ExecStart = "${lib.getExe cfg.package} serve";
        User = cfg.user;
        Group = cfg.group;
        Restart = "on-failure";
        UMask = "0077";
      };
    };
  };
}
