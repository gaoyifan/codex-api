{
  self,
  nixpkgs,
  system,
}:
let
  pkgs = import nixpkgs { inherit system; };
  testPackage = pkgs.writeShellScriptBin "codex-api" "exit 0";
  evaluated = nixpkgs.lib.nixosSystem {
    inherit system;
    modules = [
      self.nixosModules.default
      {
        system.stateVersion = "26.05";
        services.codex-api = {
          enable = true;
          package = testPackage;
          settings = {
            server.listen = "127.0.0.1:8080";
            state.path = "/var/lib/codex-api/state.sqlite3";
            upstream.auth_file = "/run/agenix/codex-api-auth";
            api_keys = [
              {
                id = "test";
                secret_file = "/run/agenix/codex-api-key";
              }
            ];
          };
        };
      }
    ];
  };
  config = evaluated.config;
in
assert
  config.systemd.services.codex-api.serviceConfig.ExecStart == "${testPackage}/bin/codex-api serve";
assert
  config.systemd.services.codex-api.environment.CODEX_API_CONFIG
  == config.environment.sessionVariables.CODEX_API_CONFIG;
assert builtins.elem testPackage config.environment.systemPackages;
assert config.users.users.codex-api.isSystemUser;
assert config.users.users.codex-api.group == "codex-api";
assert config.systemd.services.codex-api.serviceConfig.User == "codex-api";
assert config.systemd.services.codex-api.serviceConfig.Group == "codex-api";
pkgs.runCommand "codex-api-nixos-module-test" { } ''
  grep -F 'secret_file = "/run/agenix/codex-api-key"' \
    ${config.systemd.services.codex-api.environment.CODEX_API_CONFIG}
  grep -F '[model_prices."gpt-5.6-terra"]' \
    ${config.systemd.services.codex-api.environment.CODEX_API_CONFIG}
  grep -F 'input_usd_per_million = "2.00"' \
    ${config.systemd.services.codex-api.environment.CODEX_API_CONFIG}
  grep -F '[model_prices."gpt-5.6-luna"]' \
    ${config.systemd.services.codex-api.environment.CODEX_API_CONFIG}
  touch $out
''
