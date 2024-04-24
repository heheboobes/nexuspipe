CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TYPE webhook_status AS ENUM (
    'active',
    'inactive',
    'paused',
    'failed',
    'deleted'
);

CREATE TYPE delivery_status AS ENUM (
    'pending',
    'delivering',
    'delivered',
    'failed',
    'retrying',
    'cancelled'
);

CREATE TABLE webhook_configs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL,
    name            VARCHAR(255) NOT NULL,
    url             VARCHAR(2048) NOT NULL,
    secret          VARCHAR(512) NOT NULL DEFAULT '',
    status          webhook_status NOT NULL DEFAULT 'active',
    events          TEXT[] NOT NULL DEFAULT '{}',
    headers         JSONB NOT NULL DEFAULT '{}',
    content_type    VARCHAR(128) NOT NULL DEFAULT 'application/json',
    retry_enabled   BOOLEAN NOT NULL DEFAULT true,
    max_retries     INTEGER NOT NULL DEFAULT 3 CHECK (max_retries >= 0 AND max_retries <= 25),
    retry_interval  INTEGER NOT NULL DEFAULT 60 CHECK (retry_interval >= 5),
    backoff_multiplier FLOAT NOT NULL DEFAULT 2.0 CHECK (backoff_multiplier >= 1.0),
    timeout_seconds INTEGER NOT NULL DEFAULT 30 CHECK (timeout_seconds >= 1 AND timeout_seconds <= 300),
    rate_limit      INTEGER NOT NULL DEFAULT 100 CHECK (rate_limit >= 0),
    rate_interval   INTEGER NOT NULL DEFAULT 60 CHECK (rate_interval >= 1),
    ssl_verify      BOOLEAN NOT NULL DEFAULT true,
    filter_expression TEXT,
    metadata        JSONB NOT NULL DEFAULT '{}',
    created_by      VARCHAR(255) NOT NULL DEFAULT 'system',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_webhook_configs_pipeline
        FOREIGN KEY (pipeline_id)
        REFERENCES pipeline_config (id)
        ON DELETE CASCADE
);

CREATE INDEX idx_webhook_configs_pipeline ON webhook_configs (pipeline_id);
CREATE INDEX idx_webhook_configs_status ON webhook_configs (status);
CREATE INDEX idx_webhook_configs_events ON webhook_configs USING GIN (events);
CREATE INDEX idx_webhook_configs_created_at ON webhook_configs (created_at DESC);

CREATE TABLE webhook_deliveries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id      UUID NOT NULL,
    event_type      VARCHAR(128) NOT NULL,
    status          delivery_status NOT NULL DEFAULT 'pending',
    request_url     VARCHAR(2048) NOT NULL,
    request_headers JSONB NOT NULL DEFAULT '{}',
    request_body    BYTEA,
    response_status INTEGER,
    response_headers JSONB DEFAULT NULL,
    response_body   BYTEA DEFAULT NULL,
    duration_ms     INTEGER,
    attempt         INTEGER NOT NULL DEFAULT 1,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    error_message   TEXT,
    next_retry_at   TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_webhook_deliveries_webhook
        FOREIGN KEY (webhook_id)
        REFERENCES webhook_configs (id)
        ON DELETE CASCADE
);

CREATE INDEX idx_webhook_deliveries_webhook ON webhook_deliveries (webhook_id, created_at DESC);
CREATE INDEX idx_webhook_deliveries_status ON webhook_deliveries (status);
CREATE INDEX idx_webhook_deliveries_attempt ON webhook_deliveries (webhook_id, attempt);
CREATE INDEX idx_webhook_deliveries_retry ON webhook_deliveries (next_retry_at)
    WHERE status = 'retrying';
CREATE INDEX idx_webhook_deliveries_event ON webhook_deliveries (event_type);
CREATE INDEX idx_webhook_deliveries_created ON webhook_deliveries (created_at DESC);

CREATE TABLE webhook_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id      UUID NOT NULL,
    delivery_id     UUID,
    level           VARCHAR(16) NOT NULL DEFAULT 'info',
    message         TEXT NOT NULL,
    details         JSONB DEFAULT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_webhook_logs_webhook
        FOREIGN KEY (webhook_id)
        REFERENCES webhook_configs (id)
        ON DELETE CASCADE,

    CONSTRAINT fk_webhook_logs_delivery
        FOREIGN KEY (delivery_id)
        REFERENCES webhook_deliveries (id)
        ON DELETE SET NULL
);

CREATE INDEX idx_webhook_logs_webhook ON webhook_logs (webhook_id, created_at DESC);
CREATE INDEX idx_webhook_logs_delivery ON webhook_logs (delivery_id);
CREATE INDEX idx_webhook_logs_level ON webhook_logs (level);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_webhook_configs_updated_at
    BEFORE UPDATE ON webhook_configs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
