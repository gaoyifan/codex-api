DROP VIEW request_logs;

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
        ELSE printf(
            '%d.%09d',
            cost_nano_usd / 1000000000,
            cost_nano_usd % 1000000000
        )
    END AS cost_usd,
    duration_ms,
    status,
    http_status
FROM request_ledger;
