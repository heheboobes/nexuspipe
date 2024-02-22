CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TYPE pipeline_status AS ENUM (
    'draft',
    'active',
    'paused',
    'failed',
    'deleted'
);

CREATE TYPE task_status AS ENUM (
    'pending',
    'running',
    'completed',
    'failed',
    'cancelled',
    'retrying'
);

CREATE TABLE pipeline_config (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    description     TEXT NOT NULL DEFAULT '',
    status          pipeline_status NOT NULL DEFAULT 'draft',
    config_json     JSONB NOT NULL DEFAULT '{}',
    version         INTEGER NOT NULL DEFAULT 1,
    created_by      VARCHAR(255) NOT NULL DEFAULT 'system',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_pipeline_name UNIQUE (name)
);

CREATE INDEX idx_pipeline_config_status ON pipeline_config (status);
CREATE INDEX idx_pipeline_config_created_by ON pipeline_config (created_by);
CREATE INDEX idx_pipeline_config_created_at ON pipeline_config (created_at DESC);

CREATE TABLE pipeline_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL,
    status          task_status NOT NULL DEFAULT 'pending',
    trigger_type    VARCHAR(50) NOT NULL DEFAULT 'manual',
    trigger_ref     VARCHAR(255),
    input_data      JSONB,
    output_data     JSONB,
    error_message   TEXT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    duration_ms     BIGINT,
    worker_id       VARCHAR(255),
    created_by      VARCHAR(255) NOT NULL DEFAULT 'system',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_pipeline_runs_pipeline
        FOREIGN KEY (pipeline_id)
        REFERENCES pipeline_config (id)
        ON DELETE CASCADE
);

CREATE INDEX idx_pipeline_runs_pipeline_id ON pipeline_runs (pipeline_id);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs (status);
CREATE INDEX idx_pipeline_runs_created_at ON pipeline_runs (created_at DESC);
CREATE INDEX idx_pipeline_runs_worker_id ON pipeline_runs (worker_id);

CREATE TABLE task_executions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL,
    run_id          UUID NOT NULL,
    status          task_status NOT NULL DEFAULT 'pending',
    input_data      JSONB,
    output_data     JSONB,
    error_message   TEXT,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3,
    scheduled_at    TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    worker_id       VARCHAR(255),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_task_executions_pipeline
        FOREIGN KEY (pipeline_id)
        REFERENCES pipeline_config (id)
        ON DELETE CASCADE,

    CONSTRAINT fk_task_executions_run
        FOREIGN KEY (run_id)
        REFERENCES pipeline_runs (id)
        ON DELETE CASCADE
);

CREATE INDEX idx_task_executions_pipeline_id ON task_executions (pipeline_id);
CREATE INDEX idx_task_executions_run_id ON task_executions (run_id);
CREATE INDEX idx_task_executions_status ON task_executions (status);
CREATE INDEX idx_task_executions_scheduled_at ON task_executions (scheduled_at)
    WHERE status = 'pending';
CREATE INDEX idx_task_executions_worker_id ON task_executions (worker_id);

CREATE TABLE schedule_config (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id         UUID NOT NULL,
    cron_expression     VARCHAR(100) NOT NULL,
    timezone            VARCHAR(50) NOT NULL DEFAULT 'UTC',
    status              VARCHAR(20) NOT NULL DEFAULT 'active',
    enabled             BOOLEAN NOT NULL DEFAULT true,
    priority            INTEGER NOT NULL DEFAULT 100,
    max_concurrent_runs INTEGER NOT NULL DEFAULT 1,
    tags                TEXT[] DEFAULT '{}',
    metadata            JSONB DEFAULT '{}',
    next_run_time       TIMESTAMPTZ,
    last_run_time       TIMESTAMPTZ,
    created_by          VARCHAR(255) NOT NULL DEFAULT 'system',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_schedule_config_pipeline
        FOREIGN KEY (pipeline_id)
        REFERENCES pipeline_config (id)
        ON DELETE CASCADE
);

CREATE INDEX idx_schedule_config_pipeline_id ON schedule_config (pipeline_id);
CREATE INDEX idx_schedule_config_status ON schedule_config (status);
CREATE INDEX idx_schedule_config_enabled ON schedule_config (enabled, next_run_time)
    WHERE enabled = true;
CREATE INDEX idx_schedule_config_next_run ON schedule_config (next_run_time)
    WHERE enabled = true AND status = 'active';

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_pipeline_config_updated_at
    BEFORE UPDATE ON pipeline_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_pipeline_runs_updated_at
    BEFORE UPDATE ON pipeline_runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_task_executions_updated_at
    BEFORE UPDATE ON task_executions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_schedule_config_updated_at
    BEFORE UPDATE ON schedule_config
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
