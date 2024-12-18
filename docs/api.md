# NexusPipe API Reference

Base URL: `https://api.nexuspipe.local/api/v1`

All API responses follow a standard envelope:

```json
{
  "success": true,
  "data": {},
  "error": null,
  "meta": {
    "page": 1,
    "per_page": 20,
    "total": 42,
    "total_pages": 3
  },
  "request_id": "a1b2c3d4-..."
}
```

Error responses:

```json
{
  "success": false,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Invalid request body",
    "details": [
      {"field": "name", "tag": "required", "message": "name is required"}
    ]
  },
  "request_id": "e5f6g7h8-..."
}
```

Error Codes: `VALIDATION_ERROR`, `NOT_FOUND`, `CONFLICT`, `INTERNAL_ERROR`, `UNAUTHORIZED`, `FORBIDDEN`, `RATE_LIMITED`, `BAD_REQUEST`, `DEPENDENCY_FAILED`, `CONCURRENT_MODIFICATION`

---

## Health

### GET /health
Basic health check.

```json
// 200 OK
{
  "status": "ok",
  "service": "nexuspipe-api",
  "version": "0.1.0",
  "timestamp": "2026-06-25T12:00:00Z",
  "uptime": "3h12m45s"
}
```

### GET /ready
Readiness probe checking database and queue connections.

```json
// 200 OK
{
  "status": "ready",
  "checks": {
    "database": "healthy",
    "queue": "healthy"
  },
  "healthy": true
}
```

### GET /live
Liveness probe.

```json
// 200 OK
{
  "status": "alive",
  "service": "nexuspipe-api"
}
```

---

## Pipelines

### POST /pipelines
Create a new pipeline definition.

**Request:**
```json
{
  "name": "Data Ingest Pipeline",
  "description": "Processes incoming webhook data through transforms",
  "config": {
    "max_retries": 3,
    "timeout_seconds": 300,
    "concurrency": 2,
    "priority": 0,
    "queue_name": "nexuspipe.pipelines.ingest",
    "exchange_name": "nexuspipe.events",
    "routing_key": "pipeline.execute",
    "dlq_enabled": true,
    "dlq_name": "nexuspipe.pipelines.dlq",
    "retry_backoff_ms": 1000,
    "max_backoff_ms": 60000,
    "backoff_multiplier": 2.0,
    "environment": "production"
  },
  "tags": {
    "team": "data",
    "env": "prod"
  }
}
```

**Response:** `201 Created`
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Data Ingest Pipeline",
  "description": "Processes incoming webhook data through transforms",
  "status": "draft",
  "version": 1,
  "config": { ... },
  "tags": { "team": "data", "env": "prod" },
  "created_by": "00000000-0000-0000-0000-000000000001",
  "created_at": "2026-06-25T12:00:00Z",
  "updated_at": "2026-06-25T12:00:00Z"
}
```

### GET /pipelines
List pipelines with optional filters.

**Query Parameters:**
| Param    | Type   | Default | Description |
|----------|--------|---------|-------------|
| status   | string | ""      | Filter by status (active, inactive, paused, draft, archived, failed) |
| search   | string | ""      | Search by name or description |
| page     | int    | 1       | Page number |
| per_page | int    | 20      | Items per page (max 100) |

**Response:** `200 OK`
```json
{
  "data": [ ... ],
  "meta": { "page": 1, "per_page": 20, "total": 5, "total_pages": 1 }
}
```

### GET /pipelines/:id
Get a single pipeline by ID.

**Response:** `200 OK` or `404 Not Found`

### PUT /pipelines/:id
Update an existing pipeline. All fields optional.

**Request:**
```json
{
  "name": "Updated Pipeline Name",
  "status": "active",
  "config": {
    "max_retries": 5,
    "concurrency": 4
  },
  "tags": { "team": "data", "version": "2" }
}
```

**Response:** `200 OK`

### DELETE /pipelines/:id
Soft-delete a pipeline.

**Response:** `204 No Content`

### POST /pipelines/:id/execute
Trigger a pipeline execution.

**Request:**
```json
{
  "input": "{\"record_id\": 123, \"source\": \"webhook\"}",
  "triggered_by": "api"
}
```

**Response:** `200 OK`
```json
{
  "id": "660e8400-e29b-41d4-a716-446655440001",
  "pipeline_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "active",
  "input": "{\"record_id\": 123}",
  "triggered_by": "api",
  "created_at": "2026-06-25T12:00:00Z"
}
```

---

## Tasks

### GET /tasks
List tasks. Requires authentication.

**Query Parameters:**
| Param       | Type   | Default | Description |
|-------------|--------|---------|-------------|
| pipeline_id | string | ""      | Filter by pipeline |
| status      | string | ""      | Filter by status |
| page        | int    | 1       | Page number |
| per_page    | int    | 50      | Items per page (max 200) |

### GET /tasks/:id
Get a single task by ID.

### PUT /tasks/:id/retry
Retry a failed task.

**Response:** `200 OK`
```json
{
  "id": "770e8400-...",
  "status": "pending",
  "retry_count": 2
}
```

### POST /tasks/:id/cancel
Cancel a running or pending task.

**Response:** `204 No Content`

---

## Events

### POST /events
Emit a new event into the pipeline.

**Request:**
```json
{
  "event_type": "order.created",
  "source": "shopify",
  "pipeline_id": "550e8400-e29b-41d4-a716-446655440000",
  "priority": 1,
  "body": {
    "order_id": "ORD-12345",
    "customer": "john@example.com",
    "total": 49.99
  },
  "headers": {
    "content_type": "application/json",
    "idempotency_key": "idem-001",
    "trace_id": "trace-xyz"
  },
  "scheduled_at": "2026-06-25T14:00:00Z",
  "ttl": 3600
}
```

**Response:** `201 Created`
```json
{
  "id": "880e8400-...",
  "event_type": "order.created",
  "status": "pending",
  "created_at": "2026-06-25T12:00:00Z"
}
```

### GET /events
List events with filters.

**Query Parameters:**
| Param       | Type   | Description |
|-------------|--------|-------------|
| status      | string | Filter by status |
| event_types | string | Comma-separated event types |
| source      | string | Filter by source |
| pipeline_id | string | Filter by pipeline |
| from        | string | Start timestamp (RFC3339) |
| to          | string | End timestamp (RFC3339) |
| search      | string | Search body content |
| page        | int    | Page number |
| per_page    | int    | Items per page |

### GET /events/:id
Get a single event with delivery status.

### PUT /events/:id/status
Update event status.

**Request:**
```json
{
  "status": "completed"
}
```

---

## Schedules

### POST /schedules
Create a cron schedule for a pipeline.

**Request:**
```json
{
  "pipeline_id": "550e8400-e29b-41d4-a716-446655440000",
  "cron_expression": "*/5 * * * *",
  "timezone": "America/New_York",
  "priority": 0,
  "max_concurrent_runs": 1,
  "tags": ["prod", "critical"],
  "metadata": {
    "notify_on_failure": "true",
    "slack_channel": "#alerts"
  }
}
```

**Response:** `201 Created`
```json
{
  "id": "990e8400-...",
  "pipeline_id": "550e8400-...",
  "cron_expression": "*/5 * * * *",
  "status": "active",
  "enabled": true,
  "next_run_time": "2026-06-25T12:05:00Z",
  "created_at": "2026-06-25T12:00:00Z"
}
```

### GET /schedules
List all schedules.

### GET /schedules/:id
Get a schedule by ID.

### PUT /schedules/:id
Update a schedule.

### DELETE /schedules/:id
Delete a schedule.

### POST /schedules/:id/toggle
Toggle a schedule on/off.

**Response:** `200 OK`
```json
{
  "id": "990e8400-...",
  "enabled": false,
  "status": "paused"
}
```

---

## Webhooks

### POST /webhooks
Register a new webhook.

**Request:**
```json
{
  "name": "Order Webhook",
  "url": "https://hooks.example.com/nexuspipe",
  "pipeline_id": "550e8400-...",
  "secret": "whsec_abc123",
  "events": ["order.created", "order.updated"],
  "headers": {
    "X-Custom": "value"
  }
}
```

### GET /webhooks
List registered webhooks.

### GET /webhooks/:id
Get webhook details.

### PUT /webhooks/:id
Update webhook configuration.

### DELETE /webhooks/:id
Delete a webhook.

### GET /webhooks/:id/deliveries
Get delivery history for a webhook.

### POST /webhooks/:id/deliveries/:delivery_id/retry
Retry a failed webhook delivery.

---

## Admin

### GET /admin/stats
System-wide statistics. Requires admin role.

**Response:**
```json
{
  "total_pipelines": 42,
  "active_pipelines": 15,
  "total_events_24h": 12850,
  "total_tasks_24h": 38400,
  "failed_tasks_24h": 23,
  "avg_task_duration_ms": 245,
  "total_webhooks": 8,
  "total_schedules": 12,
  "queue_depth": 342,
  "worker_count": 10,
  "uptime_seconds": 11445
}
```

### GET /admin/logs
Stream recent system logs. Requires admin role.

**Query Parameters:**
| Param  | Type   | Default | Description |
|--------|--------|---------|-------------|
| level  | string | "info"  | Log level filter |
| lines  | int    | 100     | Number of lines (max 1000) |
| source | string | ""      | Component filter |
