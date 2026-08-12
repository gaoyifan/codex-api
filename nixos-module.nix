{ self }:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.services.codex-api;
  toml = pkgs.formats.toml { };
  configFile = toml.generate "codex-api.toml" cfg.settings;
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

    settings = lib.mkOption {
      type = toml.type;
      description = "Configuration written to codex-api.toml.";
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
    # Standard short-context prices: https://developers.openai.com/api/docs/pricing
    services.codex-api.settings.model_prices = lib.mkDefault {
      "gpt-5.6-sol" = {
        input_usd_per_million = "5.00";
        cached_input_usd_per_million = "0.50";
        output_usd_per_million = "30.00";
      };
      "gpt-5.6-terra" = {
        input_usd_per_million = "2.00";
        cached_input_usd_per_million = "0.20";
        output_usd_per_million = "12.00";
      };
      "gpt-5.6-luna" = {
        input_usd_per_million = "0.20";
        cached_input_usd_per_million = "0.02";
        output_usd_per_million = "1.20";
      };
    };

    users.users = lib.mkIf (cfg.user == "codex-api") {
      codex-api = {
        isSystemUser = true;
        group = cfg.group;
      };
    };
    users.groups = lib.mkIf (cfg.group == "codex-api") { codex-api = { }; };

    environment.systemPackages = [ cfg.package ];
    environment.sessionVariables.CODEX_API_CONFIG = "${configFile}";

    systemd.services.codex-api = {
      description = "ChatGPT Codex API relay";
      wantedBy = [ "multi-user.target" ];
      wants = [ "network-online.target" ];
      after = [ "network-online.target" ];
      environment.CODEX_API_CONFIG = "${configFile}";
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
