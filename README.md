# codex-api

`codex-api` is a small Rust relay for a single ChatGPT Codex subscription. It
exposes a deliberately limited OpenAI-compatible API, uses static downstream
Bearer keys from TOML, and keeps credentials, request accounting, and weekly
quota state in SQLite.

It does not provide registration, login, user management, billing, a
management UI, multi-account scheduling, or transport fallbacks.

## Public API

- `POST /v1/responses` accepts only explicit `"stream": true` and returns a
  Responses SSE stream.
- `POST /v1/chat/completions` accepts non-streaming requests and text-only
  streaming requests, translating the upstream Responses stream into Chat
  Completions JSON or SSE. `response_format` is translated to Responses
  `text.format`. `max_completion_tokens` is validated but cannot be enforced by
  the ChatGPT Codex upstream, so it is not forwarded.
- `GET /v1/responses` with WebSocket Upgrade is available when both WebSocket
  configuration switches are enabled. Each downstream socket owns exactly one
  upstream socket and supports sequential `response.create` operations.
- `GET /v1/models` and `GET /v1/models/{model}` expose the currently usable
  Codex models in OpenAI-compatible shapes. Codex metadata such as
  `context_window` is retained on each model object.

All endpoints require:

```text
Authorization: Bearer <configured-static-key>
```

The models endpoint fetches the authenticated Codex catalog and caches a
successful result in memory for one hour. It returns only upstream models with
`visibility = "list"` that also have a local `model_prices` entry. A key below
its soft limit sees that full intersection; a key in fallback state sees only
`fallback_model`; a blocked key sees an empty list. Models queries do not write
request logs or consume quota. An unavailable single model returns
`model_not_found`.

HTTP Responses and Chat requests always use upstream HTTP/SSE. Downstream
WebSockets always use an upstream WebSocket; the service never bridges or falls
back between those transports.

## Build and run

The crate requires Rust 1.94 or newer.

```bash
cargo build --release
cp config.example.toml config.toml
CODEX_API_CONFIG="$PWD/config.toml" ./target/release/codex-api serve
```

With Nix flakes enabled, the equivalent package and development workflows are:

```bash
nix build
CODEX_API_CONFIG="$PWD/config.toml" nix run . -- serve
nix develop
```

`nix flake check` builds the package and validates the flake outputs. The
integration suite uses loopback TCP sockets, which the Nix build sandbox
blocks; run `cargo test --all-targets` from `nix develop` instead.

Configuration is loaded once at startup. Restart the process after changing
it. TLS is intentionally outside the service; put it behind a trusted reverse
proxy when exposing it beyond a private network.

Every command resolves the configuration path in this order: an explicit
`--config`, `CODEX_API_CONFIG`, then `/etc/codex-api/config.toml`. For a local
checkout without installing under `/etc`, use:

```bash
export CODEX_API_CONFIG="$PWD/config.toml"
codex-api serve
codex-api logs
codex-api quota
```

`SIGINT` and `SIGTERM` stop new connections and cancel Responses, Chat, and
WebSocket operations that are still awaiting an upstream outcome. Outcomes
already received from upstream retain their terminal status and usage. The
service waits for ledger writes, connection tasks, and any dispatched OAuth
token rotation to finish before the process exits.

The ChatGPT authentication file has the same token structure as Codex CLI's
`auth.json` and must declare `"auth_mode": "chatgpt"`. It is a seed only. On
first start it is imported into SQLite; on later starts it replaces SQLite
credentials only when its `last_refresh` is strictly newer. Rotated access and
refresh tokens are stored atomically in SQLite, and the seed file is never
rewritten.

## Configuration

See [config.example.toml](config.example.toml). Important rules:

- API key IDs and resolved secrets must be non-empty and unique. Set exactly
  one of `secret` or `secret_file` for each key.
- Decimal prices and limits must be quoted strings and non-negative.
- `model_prices` supplies deterministic billing rates and bounds the models
  this relay may expose or forward. The models API additionally requires the
  model to be visible in the live Codex catalog.
- A model's optional `max_reasoning_effort` caps higher requested levels after
  fallback selection; logs record the effective model and level.
- Omitting `weekly_limit_usd` makes a key unlimited.
- Limited keys get a weekly soft limit (`weekly_limit_usd`) and a hard limit
  (`hard_limit_usd`, default `600.00`). When soft spend is exhausted and
  `fallback_model` is set, requests are rewritten to that model until hard
  spend is reached. Without `fallback_model`, soft exhaustion rejects as before.
- Enabling downstream WebSockets requires `upstream.supports_websockets = true`.
- Unknown fields fail startup, which makes configuration typos visible.

The service creates a new SQLite state file with mode `0600`, enables WAL, and
uses a bounded busy timeout. Run a single service instance against a state file.

## NixOS module

The flake exports `nixosModules.default`. The module installs the CLI, creates
the default `codex-api` service account, starts `codex-api serve`, and exports
`CODEX_API_CONFIG` to both the service and login sessions. It renders the
non-secret `settings` attribute set with `pkgs.formats.toml`; secret values stay
in agenix or sops-nix files referenced by `auth_file` and `secret_file`.

Add this flake as an input to the NixOS configuration and import its module. An
el2 host module under `~/nix/nixos/hosts/el2/` can use this shape:

```nix
{
  config,
  inputs,
  lib,
  ...
}:
{
  imports = [ inputs.codex-api.nixosModules.default ];

  age.secrets = lib.mkIf config.services.secrets.hasRealFiles {
    codex-api-auth = {
      file = config.services.secrets.filesDir + "/nixos/el2/codex-api-auth.age";
      owner = "codex-api";
      group = "codex-api";
    };
    codex-api-key = {
      file = config.services.secrets.filesDir + "/nixos/el2/codex-api-key.age";
      owner = "codex-api";
      group = "codex-api";
    };
  };

  services.codex-api = {
    enable = true;
    settings = {
      server.listen = "127.0.0.1:8080";
      state.path = "/var/lib/codex-api/state.sqlite3";
      upstream.auth_file = config.age.secrets.codex-api-auth.path;
      api_keys = [{
        id = "client-a";
        secret_file = config.age.secrets.codex-api-key.path;
      }];
    };
  };
}
```

The NixOS module supplies the current Standard short-context prices for the
GPT-5.6 Sol, Terra, and Luna models by default, and caps Sol reasoning effort
at `high`. Set `settings.model_prices` only when overriding that table
deliberately.

If `user` or `group` is changed from `codex-api`, define that account outside
this module and grant it read access to the configured secret files, plus write
access to the configured SQLite path.

## Requests

Streaming Responses:

```bash
curl -N http://127.0.0.1:8080/v1/responses \
  -H 'Authorization: Bearer sk-local-change-me' \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5.6-luna","input":"Reply with OK.","stream":true}'
```

Non-streaming Chat Completions:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'Authorization: Bearer sk-local-change-me' \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5.6-luna","messages":[{"role":"user","content":"Reply with OK."}]}'
```

The Chat compatibility layer supports ordered system, developer, user, and
assistant text; function tools and tool calls; tool results; `tool_choice`;
`parallel_tool_calls`; and `reasoning_effort`. System messages are joined with
newlines and sent as the Responses `instructions` field. Unsupported fields are
rejected instead of being silently dropped. In particular, the private ChatGPT
Codex endpoint rejects Responses `max_output_tokens`, so this relay also rejects
it and Chat `max_completion_tokens`/`max_tokens`.

For WebSocket mode, connect to `ws://host/v1/responses`, provide the same Bearer
header, and send text frames such as:

```json
{
  "type": "response.create",
  "model": "gpt-5.6-luna",
  "input": "Reply with OK."
}
```

Only one response may be in flight per connection. Validation and quota errors
are Responses `error` events and leave the socket usable. Malformed JSON is
reported the same way without contacting upstream; binary application frames
close the socket with code 1003.

## Accounting and weekly limits

Every authenticated generation operation gets a ledger row. Models queries and
invalid API keys do not.
Prices are calculated with decimal arithmetic, then rounded half-up once per
request to one nano-USD:

```text
(input - cached_input) * input_rate
+ cached_input * cached_input_rate
+ output * output_rate
```

Rates are USD per million tokens. A limited key is admitted while committed
spend in the current Monday 00:00 UTC window is below its soft
`weekly_limit_usd`. Once soft spend is exhausted, if `fallback_model` is
configured, later requests are rewritten to that model and billed at its rates
until committed spend reaches `hard_limit_usd` (default `600.00`). Past the hard
limit, or past the soft limit when no fallback model is configured, operations
receive `weekly_quota_exceeded`. An admitted request may cross a limit;
concurrent requests observe committed spend only.

The stable read-only SQLite view `request_logs` contains request metadata,
tokens, cost, duration, and status. It never contains prompts, outputs, API key
secrets, ChatGPT account IDs, tokens, or raw errors. Its columns, in order, are:

```text
id
requested_at
api_key_id
model
reasoning_effort
api_protocol
transport
input_tokens
cached_input_tokens
output_tokens
cost_usd
duration_ms
status
http_status
```

`cost_usd` is canonical decimal `TEXT` with exactly nine fractional digits, so
it preserves the ledger's nano-USD precision without a floating-point
conversion.

For interactive inspection, `logs` prints the newest 20 requests by default:

```bash
codex-api logs
codex-api logs \
  --limit 50 \
  --api-key-id client-a \
  --model gpt-5.6-luna \
  --status completed \
  --since 2026-08-10T00:00:00Z \
  --until 2026-08-11T00:00:00Z
```

The filters are combined with AND. `--since` is inclusive and `--until` is
exclusive; both accept RFC3339 timestamps at whole-second precision. Status accepts `started`, `completed`, `incomplete`, `rejected`,
`upstream_error`, `canceled`, and `internal_error`. The compact table shows API
key ID, UTC request time, model and reasoning effort, API protocol and
transport, input/cached-input/output tokens in thousands, exact USD cost,
duration, and status. Missing accounting values are shown as `—`.

`quota` prints every configured API key in configuration order for the current
UTC week, beginning Monday at 00:00. It shows exact spend, soft limit, hard
limit, remaining hard-limit headroom, and `unlimited`, `available`, `fallback`,
or `blocked` status:

```bash
codex-api quota
```

Both query commands open the existing state database read-only. They do not
create or migrate a database, initialize ChatGPT credentials, contact the
network, or listen on a port, and their output never includes configured API
key secrets or upstream credentials.

For scripts and custom queries, use the stable `request_logs` view directly:

```bash
sqlite3 ./codex-api.sqlite3 \
  'SELECT requested_at, api_key_id, model, cost_usd, status FROM request_logs ORDER BY id DESC LIMIT 20;'
```

## Verification

Offline checks use real TCP transports, temporary file-backed SQLite databases,
and local scripted upstream servers:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

The real subscription contract test is ignored by default because it makes
three billable upstream requests and uses a persistent test database:

```bash
cargo test --test live_chatgpt \
  live_chatgpt_contract_supports_responses_chat_and_websocket \
  -- --ignored --exact
```

It expects `/home/yifan/.codex-test/auth.json`, uses
`/home/yifan/.codex-test/codex-api.sqlite3`, and never prints credentials or
model output.
