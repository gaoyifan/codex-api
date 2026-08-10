# codex-api

`codex-api` is a small Rust relay for a single ChatGPT Codex subscription. It
exposes a deliberately limited OpenAI-compatible API, uses static downstream
Bearer keys from TOML, and keeps credentials, request accounting, and weekly
quota state in SQLite.

It does not provide registration, login, user management, billing, model
discovery, a management UI, multi-account scheduling, or transport fallbacks.

## Public API

- `POST /v1/responses` accepts only explicit `"stream": true` and returns a
  Responses SSE stream.
- `POST /v1/chat/completions` accepts only non-streaming requests, consumes the
  upstream Responses stream, and returns one Chat Completion JSON object.
- `GET /v1/responses` with WebSocket Upgrade is available when both WebSocket
  configuration switches are enabled. Each downstream socket owns exactly one
  upstream socket and supports sequential `response.create` operations.

All endpoints require:

```text
Authorization: Bearer <configured-static-key>
```

HTTP Responses and Chat requests always use upstream HTTP/SSE. Downstream
WebSockets always use an upstream WebSocket; the service never bridges or falls
back between those transports.

## Build and run

The crate requires Rust 1.94 or newer.

```bash
cargo build --release
cp config.example.toml config.toml
./target/release/codex-api --config ./config.toml
```

Configuration is loaded once at startup. Restart the process after changing
it. TLS is intentionally outside the service; put it behind a trusted reverse
proxy when exposing it beyond a private network.

`SIGINT` and `SIGTERM` stop new connections, cancel active Responses, Chat,
and WebSocket operations, commit their ledger rows as `canceled`, and wait for
the connection tasks and any dispatched OAuth token rotation to finish before
the process exits.

The ChatGPT authentication file has the same token structure as Codex CLI's
`auth.json` and must declare `"auth_mode": "chatgpt"`. It is a seed only. On
first start it is imported into SQLite; on later starts it replaces SQLite
credentials only when its `last_refresh` is strictly newer. Rotated access and
refresh tokens are stored atomically in SQLite, and the seed file is never
rewritten.

## Configuration

See [config.example.toml](config.example.toml). Important rules:

- API key IDs and secrets must be non-empty and unique.
- Decimal prices and limits must be quoted strings and non-negative.
- `model_prices` is the exact model allowlist.
- Omitting `weekly_limit_usd` makes a key unlimited.
- Enabling downstream WebSockets requires `upstream.supports_websockets = true`.
- Unknown fields fail startup, which makes configuration typos visible.

The service creates a new SQLite state file with mode `0600`, enables WAL, and
uses a bounded busy timeout. Run a single service instance against a state file.

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
`parallel_tool_calls`; and `reasoning_effort`. Unsupported fields are rejected
instead of being silently dropped. In particular, the private ChatGPT Codex
endpoint rejects Responses `max_output_tokens`, so this relay also rejects it
and Chat `max_completion_tokens`/`max_tokens`.

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

Every authenticated operation gets a ledger row. Invalid API keys do not.
Prices are calculated with decimal arithmetic, then rounded half-up once per
request to one nano-USD:

```text
(input - cached_input) * input_rate
+ cached_input * cached_input_rate
+ output * output_rate
```

Rates are USD per million tokens. A limited key is admitted while committed
spend in the current Monday 00:00 UTC window is below its limit. An admitted
request may cross the limit; later operations receive
`weekly_quota_exceeded`. Concurrent requests observe committed spend only.

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
conversion. For example:

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
