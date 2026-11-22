CREATE TABLE audio_request_logs (
    id INTEGER PRIMARY KEY,
    requested_at_ms INTEGER NOT NULL,
    api_key_id TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    duration_ms INTEGER NOT NULL CHECK (duration_ms >= 0),
    http_status INTEGER NOT NULL CHECK (http_status BETWEEN 100 AND 599)
) STRICT;
