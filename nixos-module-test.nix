{
  self,
  nixpkgs,
  system,
}:
let
  pkgs = import nixpkgs { inherit system; };
  testPackage = pkgs.writeShellScriptBin "codex-api" "exit 0";
  configFile = "/run/agenix/codex-api.toml";
  evaluated = nixpkgs.lib.nixosSystem {
    inherit system;
    modules = [
      self.nixosModules.default
      {
        system.stateVersion = "26.05";
        services.codex-api = {
          enable = true;
          package = testPackage;
          inherit configFile;
        };
      }
    ];
  };
  config = evaluated.config;
in
assert
  config.systemd.services.codex-api.serviceConfig.ExecStart == "${testPackage}/bin/codex-api serve";
assert config.systemd.services.codex-api.environment.CODEX_API_CONFIG == configFile;
assert config.environment.sessionVariables.CODEX_API_CONFIG == configFile;
assert builtins.elem testPackage config.environment.systemPackages;
assert config.users.users.codex-api.isSystemUser;
assert config.users.users.codex-api.group == "codex-api";
assert config.systemd.services.codex-api.serviceConfig.User == "codex-api";
assert config.systemd.services.codex-api.serviceConfig.Group == "codex-api";
pkgs.runCommand "codex-api-nixos-module-test" { } "touch $out"
