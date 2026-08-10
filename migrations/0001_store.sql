CREATE TABLE credentials (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    account_id TEXT NOT NULL,
    access_token TEXT NOT NULL,
    refresh_token TEXT NOT NULL,
    access_expires_at_ns INTEGER,
    last_refresh_ns INTEGER NOT NULL
) STRICT;

CREATE TABLE request_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    requested_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    api_key_id TEXT NOT NULL,
    model TEXT NOT NULL,
    reasoning_effort TEXT,
    api_protocol TEXT NOT NULL CHECK (api_protocol IN ('responses', 'chat_completions')),
    transport TEXT NOT NULL CHECK (transport IN ('http_sse', 'websocket')),
    input_tokens INTEGER CHECK (input_tokens IS NULL OR input_tokens >= 0),
    cached_input_tokens INTEGER CHECK (
        cached_input_tokens IS NULL
        OR (
            cached_input_tokens >= 0
            AND input_tokens IS NOT NULL
            AND cached_input_tokens <= input_tokens
        )
    ),
    output_tokens INTEGER CHECK (output_tokens IS NULL OR output_tokens >= 0),
    cost_nano_usd INTEGER CHECK (cost_nano_usd IS NULL OR cost_nano_usd >= 0),
    duration_ms INTEGER CHECK (duration_ms IS NULL OR duration_ms >= 0),
    status TEXT NOT NULL CHECK (
        status IN (
            'started',
            'completed',
            'incomplete',
            'rejected',
            'upstream_error',
            'canceled',
            'internal_error'
        )
    ),
    http_status INTEGER CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599)
) STRICT;

CREATE INDEX request_ledger_weekly_spend
    ON request_ledger (api_key_id, requested_at_ms)
    WHERE cost_nano_usd IS NOT NULL;

CREATE VIEW request_logs AS
SELECT
    id,
    strftime('%Y-%m-%dT%H:%M:%SZ', requested_at_ms / 1000.0, 'unixepoch') AS requested_at,
    api_key_id,
    model,
    reasoning_effort,
    api_protocol,
    transport,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    CASE
        WHEN cost_nano_usd IS NULL THEN NULL
        ELSE CAST(cost_nano_usd AS REAL) / 1000000000.0
    END AS cost_usd,
    duration_ms,
    status,
    http_status
FROM request_ledger;
