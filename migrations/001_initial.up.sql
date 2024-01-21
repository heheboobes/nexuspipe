CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- Pipelines
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipelines (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        VARCHAR(255) NOT NULL,
    description TEXT DEFAULT '',
    status      VARCHAR(32) NOT NULL DEFAULT 'draft'
                CHECK (status IN ('active', 'inactive', 'paused', 'archived', 'draft', 'failed')),
    version     INTEGER NOT NULL DEFAULT 1,
    config      JSONB NOT NULL DEFAULT '{}',
    tags        JSONB NOT NULL DEFAULT '{}',
    created_by  UUID NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at  TIMESTAMPTZ DEFAULT NULL
);

CREATE INDEX idx_pipelines_status ON pipelines (status) WHERE deleted_at IS NULL;
CREATE INDEX idx_pipelines_created_by ON pipelines (created_by) WHERE deleted_at IS NULL;
CREATE INDEX idx_pipelines_created_at ON pipelines (created_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX idx_pipelines_tags ON pipelines USING GIN (tags) WHERE deleted_at IS NULL;
CREATE INDEX idx_pipelines_name_search ON pipelines USING GIN (to_tsvector('english', name));

-- ============================================================================
-- Pipeline Versions
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_versions (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    version     INTEGER NOT NULL,
    config      JSONB NOT NULL DEFAULT '{}',
    changelog   TEXT DEFAULT '',
    published   BOOLEAN NOT NULL DEFAULT FALSE,
    created_by  UUID NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pipeline_id, version)
);

CREATE INDEX idx_pipeline_versions_pipeline ON pipeline_versions (pipeline_id, version DESC);

-- ============================================================================
-- Pipeline Executions
-- ============================================================================
CREATE TABLE IF NOT EXISTS pipeline_executions (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id   UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    status        VARCHAR(32) NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    input         JSONB DEFAULT NULL,
    output        JSONB DEFAULT NULL,
    error         TEXT DEFAULT NULL,
    started_at    TIMESTAMPTZ DEFAULT NULL,
    completed_at  TIMESTAMPTZ DEFAULT NULL,
    duration_ms   BIGINT DEFAULT 0,
    retry_count   INTEGER NOT NULL DEFAULT 0,
    triggered_by  VARCHAR(64) NOT NULL DEFAULT 'manual',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_pipeline_executions_pipeline ON pipeline_executions (pipeline_id, created_at DESC);
CREATE INDEX idx_pipeline_executions_status ON pipeline_executions (status);

-- ============================================================================
-- Events
-- ============================================================================
CREATE TABLE IF NOT EXISTS events (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID DEFAULT NULL REFERENCES pipelines(id) ON DELETE SET NULL,
    event_type      VARCHAR(128) NOT NULL,
    source          VARCHAR(255) NOT NULL,
    status          VARCHAR(32) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'completed', 'failed',
                                      'retrying', 'cancelled', 'delayed', 'dead_letter')),
    priority        INTEGER NOT NULL DEFAULT 0 CHECK (priority >= 0 AND priority <= 100),
    body            JSONB DEFAULT NULL,
    headers         JSONB NOT NULL DEFAULT '{}',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3 CHECK (max_retries >= 0 AND max_retries <= 25),
    correlation_id  VARCHAR(255) DEFAULT NULL,
    causation_id    VARCHAR(255) DEFAULT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at    TIMESTAMPTZ DEFAULT NULL,
    scheduled_at    TIMESTAMPTZ DEFAULT NULL,
    ttl             INTEGER DEFAULT NULL
);

CREATE INDEX idx_events_status ON events (status);
CREATE INDEX idx_events_type ON events (event_type);
CREATE INDEX idx_events_source ON events (source);
CREATE INDEX idx_events_pipeline ON events (pipeline_id) WHERE pipeline_id IS NOT NULL;
CREATE INDEX idx_events_correlation ON events (correlation_id) WHERE correlation_id IS NOT NULL;
CREATE INDEX idx_events_created ON events (created_at DESC);
CREATE INDEX idx_events_scheduled ON events (scheduled_at) WHERE scheduled_at IS NOT NULL;
CREATE INDEX idx_events_status_retry ON events (status, retry_count) WHERE status IN ('failed', 'retrying');
CREATE INDEX idx_events_headers ON events USING GIN (headers);
CREATE INDEX idx_events_body_search ON events USING GIN (body jsonb_path_ops);

-- ============================================================================
-- Event Delivery Status
-- ============================================================================
CREATE TABLE IF NOT EXISTS event_delivery_status (
    event_id     UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    status       VARCHAR(32) NOT NULL,
    consumer_id  VARCHAR(255) NOT NULL,
    delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acked_at     TIMESTAMPTZ DEFAULT NULL,
    error        TEXT DEFAULT NULL,
    attempt      INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (event_id, consumer_id, attempt)
);

CREATE INDEX idx_event_delivery_status_event ON event_delivery_status (event_id, attempt DESC);

-- ============================================================================
-- Tasks
-- ============================================================================
CREATE TABLE IF NOT EXISTS tasks (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
    event_id        UUID DEFAULT NULL REFERENCES events(id) ON DELETE SET NULL,
    parent_task_id  UUID DEFAULT NULL REFERENCES tasks(id) ON DELETE SET NULL,
    name            VARCHAR(255) NOT NULL,
    type            VARCHAR(32) NOT NULL
                    CHECK (type IN ('http', 'grpc', 'script', 'sql', 'shell',
                                    'webhook', 'transform', 'notification', 'custom')),
    status          VARCHAR(32) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'scheduled', 'running', 'completed', 'failed',
                                      'cancelled', 'retrying', 'paused', 'timed_out', 'skipped')),
    priority        INTEGER NOT NULL DEFAULT 0 CHECK (priority >= 0 AND priority <= 100),
    sequence        INTEGER NOT NULL DEFAULT 0,
    input           JSONB DEFAULT NULL,
    output          JSONB DEFAULT NULL,
    error           TEXT DEFAULT NULL,
    config          JSONB NOT NULL DEFAULT '{}',
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 3 CHECK (max_retries >= 0 AND max_retries <= 25),
    timeout_seconds INTEGER NOT NULL DEFAULT 60 CHECK (timeout_seconds > 0),
    worker_id       VARCHAR(128) DEFAULT NULL,
    started_at      TIMESTAMPTZ DEFAULT NULL,
    completed_at    TIMESTAMPTZ DEFAULT NULL,
    duration_ms     BIGINT DEFAULT 0,
    scheduled_at    TIMESTAMPTZ DEFAULT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_tasks_pipeline ON tasks (pipeline_id, sequence);
CREATE INDEX idx_tasks_event ON tasks (event_id) WHERE event_id IS NOT NULL;
CREATE INDEX idx_tasks_parent ON tasks (parent_task_id) WHERE parent_task_id IS NOT NULL;
CREATE INDEX idx_tasks_status ON tasks (status);
CREATE INDEX idx_tasks_type ON tasks (type);
CREATE INDEX idx_tasks_worker ON tasks (worker_id) WHERE worker_id IS NOT NULL;
CREATE INDEX idx_tasks_priority ON tasks (priority DESC, created_at ASC) WHERE status = 'pending';
CREATE INDEX idx_tasks_created ON tasks (created_at DESC);
CREATE INDEX idx_tasks_scheduled ON tasks (scheduled_at) WHERE scheduled_at IS NOT NULL AND status = 'scheduled';
CREATE INDEX idx_tasks_stale ON tasks (status, updated_at) WHERE status = 'running';

-- ============================================================================
-- Task Dependencies
-- ============================================================================
CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id     UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on  UUID NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    mandatory   BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (task_id, depends_on),
    CONSTRAINT chk_no_self_dependency CHECK (task_id <> depends_on)
);

CREATE INDEX idx_task_dependencies_task ON task_dependencies (task_id);
CREATE INDEX idx_task_dependencies_depends ON task_dependencies (depends_on);
