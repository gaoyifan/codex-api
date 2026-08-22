# Simplified Codex API Relay Implementation Plan

## 1. Summary

Build a Rust 2024 service named `codex-api` that relays requests to one ChatGPT Codex subscription. The service is configured entirely through TOML, stores mutable state and request accounting in SQLite, and has no users, registration, login, management UI, Redis, model discovery, or multi-account scheduling.

Public API surface:

- `POST /v1/responses`: streaming only. The request must explicitly contain `stream: true`; the response is a semantically equivalent Responses SSE stream.
- `GET /v1/responses` with WebSocket Upgrade: optional standard Responses WebSocket transport using `response.create` requests and Responses event messages.
- `POST /v1/chat/completions`: non-streaming only. The service converts the Chat Completions request to a streaming Responses request, consumes the upstream stream, and returns one Chat Completion object.

Transport policy:

- HTTP Responses and Chat Completions always use the upstream HTTP/SSE transport.
- Each downstream WebSocket connection maps one-to-one to an upstream WebSocket connection.
- Do not bridge downstream WebSockets to upstream SSE, and do not silently fall back from WebSocket to SSE.
- Enabling the downstream WebSocket endpoint requires `upstream.supports_websockets = true`.

Use mature libraries for transport, framing, persistence, parsing, money arithmetic, and secret handling: Axum, Tokio, Reqwest, `eventsource-stream`, Tokio Tungstenite, SQLx SQLite, Serde/TOML, `rust_decimal`, and Secrecy. Use New API only as a behavioral reference; do not copy its AGPL-licensed implementation.

Protocol baselines:

- OpenAI Responses streaming and WebSocket documentation.
- The Chat Completions and Responses schemas and migration guidance.
- The ChatGPT subscription behavior in the official Codex CLI `rust-v0.147.0` source, including the private subscription endpoint, required headers, OAuth refresh behavior, and current WebSocket beta header.

## 2. Configuration and Command-Line Interface

Initialize a Rust binary crate with an explicit subcommand:

```text
codex-api serve
```

The command-line interface also provides human-oriented, read-only state
queries:

```text
codex-api logs [--limit N] [--api-key-id ID] [--model MODEL] [--status STATUS] [--since RFC3339] [--until RFC3339]
codex-api quota
```

All commands resolve configuration in this order: explicit `--config`, the
`CODEX_API_CONFIG` environment variable, then
`/etc/codex-api/config.toml`.

`logs` defaults to the newest 20 rows ordered by ledger ID descending. Filters
are combined with AND; `since` is inclusive and `until` is exclusive, and both
accept RFC3339 timestamps at whole-second precision. Accepted
statuses are `started`, `completed`, `incomplete`, `rejected`,
`upstream_error`, `canceled`, and `internal_error`. Its compact table contains
API key ID, UTC time, model/reasoning, protocol/transport,
input/cached-input/output tokens in thousands, exact USD cost, duration, and
status. Null accounting values display as `—`.

`quota` lists every configured API key in configuration order for the current
UTC week beginning Monday at 00:00, including spend, soft limit, hard limit,
remaining hard-limit headroom, and `unlimited`, `available`, `fallback`, or
`blocked` status. It sums non-null persisted cost values exactly. A limited key
is `available` below its soft limit, `fallback` between soft and hard when
`fallback_model` is configured, and `blocked` at or above the hard limit (or at
or above the soft limit when no `fallback_model` is configured). Remaining is
always relative to the hard limit for limited keys (zero only when hard spend
is exhausted).

Both queries load configuration but open the existing SQLite database
read-only. They do not create or migrate it, initialize ChatGPT credentials,
contact the network, or listen on a port, and never print API key secrets or
upstream credentials. The CLI is for interactive inspection; the stable
`request_logs` view remains the scripting and custom-query interface.

Export a `nixosModules.default` flake output with a
`services.codex-api` module. The module installs the package, manages the
default static service user/group, runs the explicit `serve` subcommand, and
sets `CODEX_API_CONFIG` for the systemd service and interactive sessions. Its
`settings` option is rendered with `pkgs.formats.toml`; the generated store file
contains only non-secret settings and runtime paths to agenix or sops-nix
credential files.

Configuration is read once at startup. Changes require a restart. Provide a redacted `config.example.toml` with this shape:

```toml
fallback_model = "example-model" # optional; rewrite target after soft quota

[server]
listen = "127.0.0.1:8080"
enable_websockets = true

[state]
path = "./codex-api.sqlite3"

[upstream]
base_url = "https://chatgpt.com/backend-api/codex"
oauth_token_url = "https://auth.openai.com/oauth/token"
auth_file = "/path/to/auth.json"
supports_websockets = true

[[api_keys]]
id = "client-a"
secret_file = "/run/agenix/codex-api-key"
weekly_limit_usd = "10.00" # optional soft weekly limit; omission means unlimited
# hard_limit_usd = "600.00" # optional; defaults to 600.00 when weekly_limit_usd is set

[model_prices.example-model]
input_usd_per_million = "1.00"
cached_input_usd_per_million = "0.10"
output_usd_per_million = "6.00"
```

Configuration rules:

- API key IDs and resolved secrets must be non-empty and unique. Each key sets
  exactly one of `secret` or `secret_file`.
- API key secrets remain in configuration or their referenced files and in
  memory only; never persist them to SQLite.
- `model_prices` is also the model allowlist. Every request must name an exact configured model so every request log has a deterministic cost, including requests made with an unlimited key.
- Optional top-level `fallback_model` must be non-empty and name a model present in `model_prices`. When a limited key's soft weekly spend is exhausted, requests are rewritten to this model until the hard weekly limit is reached.
- Limited keys may set `hard_limit_usd` (default `600.00`, must be >= `weekly_limit_usd`). When `fallback_model` is set, hard must be strictly greater than soft so a fallback window exists. Unlimited keys must not set `hard_limit_usd`.
- Money values are decimal strings and must be non-negative.
- Reject unknown configuration fields so configuration typos fail before the server starts listening.
- If downstream WebSockets are enabled while upstream WebSockets are unsupported, fail startup.
- The production URLs have the defaults shown above, while configurable URLs allow the test suite to use local HTTP, WebSocket, and OAuth servers.

Do not add configuration hot reload, dynamic API key management, a pricing fetcher, model aliases, wildcard prices, feature flags, or compatibility modes.

On `SIGINT` or `SIGTERM`, stop accepting new connections, cancel active HTTP
SSE, Chat aggregation, and WebSocket operations, commit their ledger rows as
`canceled`, wait for upgraded WebSocket tasks and any already-dispatched OAuth
rotation to finish, and then exit.

## 3. SQLite State and Public Request Log

Use SQLx migrations and a file-backed SQLite database. Create a new database file with mode `0600`. Configure SQLite for concurrent asynchronous access with WAL and a bounded busy timeout.

### 3.1 Credential state

Keep credential tables internal. Persist the current:

- ChatGPT account ID
- access token
- refresh token
- access-token expiry, when it can be decoded
- last refresh time

Only accept an authentication seed with `auth_mode: "chatgpt"`; other Codex
authentication modes are out of scope. Credential precedence on startup:

1. Import `auth.json` when no credential row exists.
2. On later starts, compare `auth.json.last_refresh` to the SQLite refresh time.
3. Replace SQLite state only when the file is strictly newer.
4. Otherwise retain SQLite state, preventing a stale auth file from restoring an invalidated refresh token.
5. Never rewrite `auth.json`.

Write access-token and rotated refresh-token updates in one transaction. The supplied authentication file is a seed, while SQLite is the normal runtime authority.

### 3.2 Request ledger and weekly quotas

Insert one request record after a downstream API key authenticates successfully. Do not persist attempts made with missing or invalid API keys, which avoids an unauthenticated database-filling vector.

For WebSockets, record each `response.create` operation separately rather than recording one row per connection. A valid key's rejected validation and quota attempts are also recorded.

The internal ledger must capture:

- request start and finish timestamps
- stable API key ID, never the secret or a recoverable representation of it
- requested model and reasoning effort
- API protocol: `responses` or `chat_completions`
- upstream transport: `http_sse` or `websocket`
- input, cached-input, and output token counts
- cost in integer nano-USD
- duration in milliseconds
- semantic status
- HTTP status when one exists

Supported semantic statuses are:

- `started`
- `completed`
- `incomplete`
- `rejected`
- `upstream_error`
- `canceled`
- `internal_error`

Rows left as `started` after an abrupt process death are retained as evidence of interruption. Do not add speculative startup cleanup.

Expose a stable, read-only SQLite view named `request_logs` with these columns:

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

Treat this view as a public read-only interface and test it directly. Credential tables and the underlying request table remain implementation details. Do not expose request logs through HTTP and do not add quota response headers.

Expose `cost_usd` as canonical decimal `TEXT` with exactly nine fractional
digits. Derive it from the integer nano-USD value without a floating-point
conversion.

Never store prompts, outputs, ChatGPT account identifiers, raw errors, or authentication values in request logs. Retain request logs permanently in v1; do not implement cleanup or retention jobs.

### 3.3 Cost and quota semantics

Calculate each request's price as:

```text
(input_tokens - cached_input_tokens) * input_rate
+ cached_input_tokens * cached_input_rate
+ output_tokens * output_rate
```

Rates are USD per million tokens. Use exact decimal arithmetic and round half-up once per request to one nano-USD before persisting. Reasoning tokens are already included in output tokens and are not charged again. When cached-token details are absent, treat cached tokens as zero.

The weekly window begins Monday at 00:00 UTC. Attribute cost to the week in which the request started. Compute current spend from committed ledger rows with a non-null cost in the current window; do not recompute historic rows after prices change.

Admission behavior:

- An unlimited key is always admitted after validation.
- A limited key is admitted with the requested model while committed current-week spend is strictly below its soft `weekly_limit_usd`.
- Once soft spend is exhausted, if `fallback_model` is configured and committed spend is still below `hard_limit_usd`, admit the request but rewrite the upstream model (and billing rates) to `fallback_model`. The ledger stores the effective model.
- Once hard spend is exhausted, or soft spend is exhausted without a configured `fallback_model`, later requests receive an OpenAI-style HTTP `429` error with code `weekly_quota_exceeded`.
- The admitted request may push the total over a limit.
- Concurrent requests independently observe committed spend. Do not invent reservations based on unknown future token usage.
- For an upgraded WebSocket, check quota before every `response.create`. A rejected operation receives a standard Responses `error` event and the connection stays open.
- Charge any terminal response that reports usage, including incomplete or failed responses. Failed/error responses without reported usage have a null cost; completed and incomplete terminal responses must report usage. Once the relay receives an upstream terminal outcome, preserve that status and accounting even if the downstream disconnects before delivery; record `canceled` only while the upstream operation is still awaiting an outcome.

## 4. Authentication and Upstream Credential Refresh

All downstream HTTP and WebSocket clients authenticate with:

```text
Authorization: Bearer <configured-static-key>
```

Return an OpenAI-style `401` error for missing, malformed, or incorrect credentials. Use the stable API key ID for accounting after successful authentication.

The ChatGPT subscription upstream is:

```text
https://chatgpt.com/backend-api/codex/responses
wss://chatgpt.com/backend-api/codex/responses
```

Send the current access token as Bearer authentication and the configured account as `ChatGPT-Account-ID`. Mirror the stable headers required by the current official Codex client, including its originator, version/User-Agent behavior, SSE accept header, and the current Responses WebSocket beta header. Do not forward the downstream authorization header upstream.

On Responses HTTP, Chat Completions, and WebSocket requests, forward downstream values for the New API Codex allowlist: `Originator`, `Session_id`, `Thread_id`, `Session-Id`, `Thread-Id`, `X-Client-Request-Id`, `User-Agent`, `X-Codex-Beta-Features`, `X-Codex-Turn-State`, `X-Codex-Turn-Metadata`, `X-Codex-Window-Id`, `X-Codex-Parent-Thread-Id`, `X-OpenAI-Subagent`, `X-OpenAI-Memgen-Request`, `X-ResponsesAPI-Include-Timing-Metrics`, and `X-OpenAI-Internal-Codex-Responses-Lite`. Apply this allowlist without a `prompt_cache_key` gate. Keep the relay defaults when `Originator` or `User-Agent` is absent, and do not forward `X-Codex-Installation-Id` or `X-OAI-Attestation`.

Refresh behavior:

- Decode the trusted access token's JWT payload to discover expiry; signature verification is not needed because this is expiry inspection, not authentication.
- Refresh proactively when expiry is within five minutes.
- If expiry cannot be decoded, refresh when `last_refresh` is older than eight days, matching current Codex behavior.
- Serialize refreshes with one single-flight lock because ChatGPT refresh tokens rotate and reuse can invalidate them.
- Call the configured OAuth token endpoint with the current official Codex client ID and refresh-token grant.
- Preserve an existing token field when the OAuth response legitimately omits that optional field.
- Commit all returned token changes and the new refresh time atomically.
- When the initial HTTP request or WebSocket handshake returns 401 before any downstream response has begun, refresh and retry once.
- Do not retry other failures, retry a started stream, or fall back between WebSocket and SSE.
- Report an exhausted upstream authentication failure as a gateway error; never present it as a downstream API-key failure.

## 5. Responses HTTP/SSE Behavior

`POST /v1/responses` must:

- require a JSON body with an exact configured `model`
- require explicit `stream: true`; missing, null, or false is a local `400 invalid_request_error`
- reject `store: true` and background mode
- reject `max_output_tokens`, which the subscription HTTP endpoint does not support
- send `store: false` upstream
- otherwise preserve supported Responses request fields rather than defining an unnecessarily narrow DTO
- preserve the upstream non-2xx status and standard error body before streaming starts

The service must parse upstream SSE so it can inspect terminal usage and record accounting, then emit canonical SSE framing downstream. Preserve event names, event IDs, data JSON, order, and streaming timeliness. Do not promise byte-identical network chunks, and do not add a Chat Completions-style `[DONE]` marker.

Forward unknown informational Responses events unchanged. Recognize completed, incomplete, failed, and error terminal events for accounting. Before emitting a terminal event, commit final usage, price, duration, and status to SQLite. If the terminal event is malformed, lacks required usage for a priced completion, or cannot be committed, close the stream without fabricating a successful terminal event.

If the client disconnects before an upstream terminal outcome arrives, cancel the upstream request and finish the ledger row as canceled. Once HTTP SSE headers have been sent, do not attempt to change the HTTP status in response to a later stream error.

## 6. Chat Completions Compatibility

Use the standard path `POST /v1/chat/completions`; treat `chat_completation` in the original request as a spelling error, not as an alias.

Accepted v1 subset:

- required `model` and `messages`
- `stream` omitted, null, or false
- `n` omitted, null, or one
- ordered system, developer, user, and assistant text messages
- string content and standard text content parts
- function tools
- assistant function tool calls
- tool-result messages linked by `tool_call_id`
- `tool_choice`: none, auto, required, or one named function
- `parallel_tool_calls`
- `reasoning_effort`

Reject, without silently dropping:

- `stream: true` or `n != 1`
- image, audio, file, and input-audio content
- structured response formats and verbosity controls
- legacy `functions`, `function_call`, and `role: function`
- custom tools and allowed-tools extensions
- output-token limits (`max_completion_tokens`, deprecated `max_tokens`, and
  Responses `max_output_tokens`), which the ChatGPT Codex subscription endpoint
  rejects
- stop sequences, logprobs, logit bias, penalties, seed, prediction, web-search options, and all other unsupported request fields

Request conversion:

- Preserve message order and roles rather than merging system/developer messages into one instruction string.
- Convert input text parts to Responses input text.
- Convert assistant tool calls to Responses `function_call` items.
- Convert tool messages to `function_call_output` items using `tool_call_id` as `call_id`.
- Flatten Chat function tool and named tool-choice wrappers to their Responses representations.
- Send `strict: false` when Chat omitted it, because the default semantics differ.
- Map `reasoning_effort` to `reasoning.effort`.
- Force upstream `stream: true` and `store: false`.

Consume the complete upstream stream without returning partial output. Prefer a
non-empty full output carried by `response.completed` or `response.incomplete`.
The private Codex endpoint normally delivers full items in
`response.output_item.done` and leaves terminal output empty, so retain those
completed items as the fallback output source. Never reconstruct output from
text or argument deltas.

Response conversion:

- Concatenate output-text parts in response order.
- Map refusals to the Chat message refusal field.
- Map Responses function calls to Chat tool calls in output order.
- Ignore reasoning items rather than exposing private reasoning.
- Reject unsupported hosted/custom/program/shell output items with a 502 conversion error.
- Use `tool_calls` as finish reason when a completed response contains function calls; otherwise use `stop`.
- Map max-output-token incompletion to `length` and content filtering to `content_filter`.
- Map input, cached-input, output, total, and reasoning token details to the corresponding Chat usage fields.
- Reuse the upstream response ID, model, and creation timestamp; return one choice with index zero.
- Treat an error event, failed response, missing terminal response, malformed terminal response, or missing usage as an upstream/protocol error rather than fabricating a completion.

Record local validation failures, quota failures, upstream failures, incompletions, client cancellation, token usage, cost, and duration in the SQLite ledger after downstream authentication succeeds.

## 7. Responses WebSocket Behavior

When enabled, `GET /v1/responses` upgrades authenticated callers to the standard Responses WebSocket protocol.

Connection behavior:

- Establish exactly one upstream WebSocket for each downstream WebSocket.
- Authenticate only the upstream side with the ChatGPT subscription credentials.
- Send the current official Responses WebSocket beta header upstream.
- Do not establish an SSE request for a downstream WebSocket.
- Support multiple sequential turns on one connection, including `previous_response_id` and warm-up requests.
- Allow only one in-flight response per connection. Reject a second `response.create` with a Responses error event without forwarding it.
- Pin authentication, API key ID, and upstream connection for the life of the downstream connection.

For each downstream text message:

- require `type: "response.create"`
- require an exact configured model
- reject transport-specific `stream` and `background` fields
- reject `store: true` and send `store: false` upstream
- reject `max_output_tokens`, which the subscription WebSocket endpoint does not support
- add only the upstream-private fields currently required by the Codex WebSocket wire contract
- check quota and create a separate ledger row

Forward upstream text event frames in order. Inspect terminal frames before forwarding them so accounting is committed first. Unknown informational event types remain unchanged.

Handle control and failure behavior as follows:

- Forward or respond correctly to ping, pong, and close frames.
- Reject malformed JSON text with a standard Responses `error` event while keeping the connection usable; reject binary application messages with close code 1003.
- On downstream cancellation, close the upstream socket and mark an in-flight operation canceled.
- On an abnormal upstream close or protocol failure, complete the ledger row as an upstream error and close downstream with 1011.
- On validation or quota rejection after upgrade, send a standard Responses `error` event and keep the socket open for a later valid operation.

## 8. TDD Workflow and Multi-Agent Execution

The test seams are fixed as follows:

- real binary processes for startup and shutdown behavior
- real TCP HTTP/SSE and WebSocket requests for public API behavior
- a local scripted Axum server for ChatGPT HTTP/SSE, WebSocket, and OAuth boundaries
- real temporary file-backed SQLite databases
- restart-based observation for internal credential state
- a controlled clock injected through the library `run_with_clock` boundary for
  expiry, duration, and week-boundary behavior; the binary uses the system clock
- real binary processes for the human-oriented `logs` and `quota` query CLI
- direct read-only SQL against the public `request_logs` view for scripted and
  custom inspection

Do not mock internal modules, inspect private credential tables from tests, or make routers, middleware, converters, and SQL queries public merely for testing.

Honor the requested tests-first sequence:

1. Initialize a compilable Rust binary/library skeleton and test-support code without feature behavior.
2. Use sub-agents to write non-overlapping black-box test files in parallel.
3. Complete the entire agreed behavioral suite before writing production behavior.
4. Run it and confirm failures are caused by missing behavior, not broken fixtures.
5. Freeze the public contracts, then implement in vertical groups until the existing red suite becomes green.

Suggested non-overlapping test workstreams:

- process startup, configuration, downstream authentication, quota, and request-log view
- Responses HTTP/SSE behavior and cancellation
- Chat request/response conversion
- OAuth refresh, SQLite restart persistence, and single-flight concurrency
- Responses WebSocket behavior and lifecycle
- ignored live upstream contract test

After the test suite is complete, sub-agents may implement separate config/state, protocol, and upstream transport modules while the primary agent owns application assembly, routing, and final integration. No two agents should edit the same source file concurrently.

## 9. Test Matrix

### Startup and configuration

- missing configuration, missing required fields, unreadable auth seed, invalid auth structure, invalid prices, duplicate API keys, impossible WebSocket configuration, and unwritable SQLite all fail before listening
- errors and operational logs never reveal secrets
- valid configuration starts and serves both HTTP endpoints
- omitting the required subcommand fails, while `serve` starts the relay
- configuration is discovered from `CODEX_API_CONFIG`, an explicit
  `--config` overrides it, and the documented `/etc` default is advertised
- the NixOS module evaluates to the documented service command, runtime and
  session environment, installed package, and static service account

### Query CLI

- `logs` defaults to the latest 20 rows in reverse ledger order and renders the documented compact columns and null markers
- key, model, status, inclusive `since`, exclusive `until`, and positive limit filters work independently and together; invalid filters fail with actionable errors
- `quota` lists all configured keys in order and reports exact current-UTC-week spend, soft/hard limits, remaining hard-limit headroom, and unlimited/available/fallback/blocked state
- both commands work while the relay owns a WAL database, make no network requests, do not initialize credentials, do not modify or create state, and never reveal secrets

### Downstream authentication

- missing, malformed, and incorrect Bearer headers return 401 without contacting upstream or inserting request rows
- every valid key works on Responses, Chat Completions, and WebSocket handshake
- rotating a configured secret while keeping the same API key ID preserves weekly spend

### Responses HTTP/SSE

- only explicit `stream: true` is accepted
- unsupported storage/background combinations are rejected before upstream access
- expected subscription headers reach the fake upstream; the downstream key does not
- the first downstream event arrives before upstream completion, proving the stream is not buffered
- arbitrarily split SSE chunks preserve event semantics and order
- unknown informational events and Codex extension events are preserved
- upstream non-2xx errors, malformed events, missing terminal events, early EOF, and client cancellation have deterministic outcomes
- terminal usage is committed before terminal delivery

### Chat conversion

- every supported role and content representation converts correctly and preserves order
- function definitions, named/automatic/required tool choice, assistant tool calls, multiple calls, and tool outputs convert correctly
- omitted function strictness becomes `strict: false`
- reasoning effort maps correctly and unsupported token limits are rejected
- output text, refusal, function calls, creation metadata, finish reason, and usage map correctly
- completed, tool-call, length, content-filter, upstream failure, malformed terminal, unsupported output item, and no-terminal cases are covered
- every unsupported Chat field is rejected and never silently omitted
- no partial JSON is returned before the terminal response

### OAuth and credential persistence

- access tokens refresh five minutes before expiry
- undecodable token expiry uses the eight-day refresh-age rule
- concurrent expired requests perform exactly one refresh
- rotated refresh tokens are committed atomically
- restart uses newer SQLite state instead of an older auth seed
- a strictly newer auth seed replaces SQLite state
- one pre-stream 401 triggers exactly one refresh and retry
- repeated 401 and refresh failures do not trigger loops or unrelated fallbacks

### Pricing, quota, and request log

- uncached input, cached input, and output rates produce the exact expected nano-USD amount
- pricing rounds once per request with the specified rule
- UTC Monday rollover starts a new window
- a request may cross the limit and later requests receive 429
- concurrent requests observe committed spend without speculative reservation
- incomplete or failed responses with usage are charged
- validation failures, quota rejections, upstream errors, cancellations, and successes produce correct semantic statuses
- every public `request_logs` column is verified through the view, including model, reasoning effort, protocol, transport, token breakdown, cost, duration, and HTTP status
- no row contains secrets, prompts, output, account IDs, or raw error text
- restart preserves logs and current-week quota consumption

### WebSocket

- disabled WebSocket routing and invalid configuration are deterministic
- handshake authentication occurs before upgrade
- one downstream socket creates one upstream socket with the expected subscription and beta headers
- request and event text frames preserve their semantic payload and order
- sequential responses and `previous_response_id` remain on the same upstream connection
- a second in-flight request is rejected locally
- each operation receives its own quota check and request log row
- quota and validation errors keep the connection open
- normal close, cancellation, binary input, malformed text, upstream abnormal close, and refresh-after-handshake-401 are covered

### Live ChatGPT contract test

Commit one ignored integration flow that explicitly uses:

```text
/home/yifan/.codex-test/auth.json
/home/yifan/.codex-test/codex-api.sqlite3
gpt-5.6-luna
```

The persistent live-test SQLite file prevents a rotated refresh token from being discarded while leaving the supplied auth file unchanged. Run the live flow explicitly during development acceptance; normal `cargo test` must not consume subscription quota.

The live flow performs one minimal Responses HTTP request, one Chat conversion request, and one Responses WebSocket request with a short deterministic prompt and the lowest appropriate reasoning effort. Assert authentication, terminal event/completion, usage, and request-log fields only. Never print tokens, request headers, account identifiers, or full model responses.

## 10. Implementation Order and Final Verification

After the full suite exists and is red, implement in this order:

1. crate structure, explicit serve/query CLI parsing, TOML loading, startup validation, and graceful shutdown
2. SQLite migrations, credential import precedence, request ledger, and public view
3. downstream static-key authentication and OpenAI-style local errors
4. upstream HTTP/SSE client and Responses handler
5. Chat request conversion, terminal aggregation, and response conversion
6. OAuth refresh, single-flight coordination, 401 recovery, and restart persistence
7. exact pricing, weekly admission, and all request-finalization paths
8. one-to-one Responses WebSocket proxy and per-operation accounting
9. live ChatGPT contract validation and documentation

Final validation commands:

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
```

Then explicitly run the ignored live contract test once with `gpt-5.6-luna`.

Deliver:

- Rust source and committed `Cargo.lock`
- SQLx migrations
- `config.example.toml`
- README with configuration, explicit serve and query commands, API examples, quota semantics, request-log schema, and live-test command
- `.gitignore` excluding credentials, SQLite databases, WAL/SHM files, and build output

Do not add TLS termination, Docker packaging, configuration reload, registration/login, management APIs, `/v1/models`, multi-account scheduling, log cleanup, speculative retries, feature flags, compatibility layers, or other unrequested protocol coverage in v1.
