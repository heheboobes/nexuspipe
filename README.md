# NexusPipe

**Distributed Event Processing Pipeline**

NexusPipe is a high-throughput, fault-tolerant event processing pipeline built with Go. It uses PostgreSQL for persistence, RabbitMQ for message brokering, and Redis for caching and rate limiting. The system is designed for horizontal scalability and production-grade observability.

---

## Architecture

```
                     ┌─────────────┐
                     │   Clients   │
                     └──────┬──────┘
                            │ HTTP/gRPC
                     ┌──────▼──────┐
                     │  API Server │  (cmd/api)
                     └──────┬──────┘
                            │ Events
                     ┌──────▼──────┐
                     │   RabbitMQ  │  Exchange + Queues
                     └──┬──────┬───┘
                        │      │
              ┌─────────▼┐  ┌──▼──────────┐
              │  Workers │  │  Scheduler  │  (cmd/worker, cmd/scheduler)
              └──┬──────┬┘  └──────┬──────┘
                 │      │          │
         ┌───────▼┐  ┌──▼────┐    │
         │   PG   │  │ Redis │    │
         └────────┘  └───────┘    │
                        │         │
              ┌─────────▼─────────▼──────┐
              │      PostgreSQL (state)   │
              └──────────────────────────┘
```

## Components

| Component | Directory | Description |
|-----------|-----------|-------------|
| **API Server** | `cmd/api/` | RESTful API for managing pipelines, events, and tasks via Gin |
| **Worker** | `cmd/worker/` | Background consumer that processes events/tasks from RabbitMQ queues |
| **Scheduler** | `cmd/scheduler/` | Cron-based scheduler for periodic pipeline execution and maintenance |
| **Migrator** | `cmd/migrator/` | Database migration tool using golang-migrate |

## Features

- **Pipeline Management** - Create, version, and execute multi-step data pipelines
- **Event-Driven Architecture** - Publish and consume events with at-least-once delivery guarantees
- **Task Orchestration** - Chain tasks with dependency resolution and conditional execution
- **Horizontal Scaling** - Workers scale horizontally with configurable concurrency per queue
- **Dead Letter Queues** - Automatic DLQ routing for failed messages with retry policies
- **Structured Logging** - Zero-allocation structured logging via zap with sampling support
- **Metrics** - Prometheus metrics for request latency, throughput, error rates, and queue depth
- **Graceful Shutdown** - Signal-based shutdown with configurable grace periods
- **Health Checks** - Liveness and readiness endpoints for Kubernetes deployments
- **JWT Authentication** - Token-based auth with configurable TTL and refresh tokens
- **Rate Limiting** - Per-client rate limiting backed by Redis
- **Retry with Backoff** - Exponential backoff with configurable multiplier and max backoff
- **Event Correlation** - Distributed tracing via correlation/causation IDs across events

## Getting Started

### Prerequisites

- Go 1.22+
- PostgreSQL 15+
- RabbitMQ 3.12+
- Redis 7+

### Configuration

Copy the example config and edit:

```bash
cp config.example.yaml config.yaml
```

NexusPipe uses Viper for configuration. Values can be set via YAML file, environment variables (prefix `NEXUSPIPE_`), or CLI flags.

### Running Migrations

```bash
# Apply all pending migrations
make migrate-up

# Rollback the last migration
make migrate-down

# Reset all migrations
go run ./cmd/migrator --config=config.yaml reset
```

### Starting Services

```bash
# Start the API server
make run-api

# Start the worker pool
make run-worker

# Start the scheduler
make run-scheduler
```

### Building

```bash
# Build all binaries
make build

# Build Docker images
make docker-build
```

## API Endpoints

### Health
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/health` | Liveness check |
| GET | `/api/v1/ready` | Readiness check |
| GET | `/metrics` | Prometheus metrics |

### Pipelines
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/pipelines` | Create a pipeline |
| GET | `/api/v1/pipelines` | List pipelines |
| GET | `/api/v1/pipelines/:id` | Get pipeline details |
| PUT | `/api/v1/pipelines/:id` | Update pipeline |
| DELETE | `/api/v1/pipelines/:id` | Delete pipeline |
| POST | `/api/v1/pipelines/:id/execute` | Execute a pipeline |

### Events
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/events` | Emit an event |
| GET | `/api/v1/events` | List events |
| GET | `/api/v1/events/:id` | Get event details |
| PUT | `/api/v1/events/:id/status` | Update event status |

### Tasks
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/tasks` | List tasks |
| GET | `/api/v1/tasks/:id` | Get task details |
| PUT | `/api/v1/tasks/:id/retry` | Retry a failed task |
| POST | `/api/v1/tasks/:id/cancel` | Cancel a task |

## Project Structure

```
nexuspipe/
├── cmd/
│   ├── api/            # API server entrypoint
│   ├── worker/         # Background worker entrypoint
│   ├── scheduler/      # Cron scheduler entrypoint
│   └── migrator/       # DB migration tool
├── internal/
│   ├── config/         # Viper-based configuration
│   ├── logger/         # Zap structured logger
│   └── models/         # Domain models (pipeline, event, task)
├── migrations/         # SQL migration files
├── api/proto/          # Protobuf definitions (future)
├── deployments/        # Dockerfiles and K8s manifests
├── go.mod
├── go.sum
├── Makefile
└── config.example.yaml
```

## Observability

NexusPipe exports structured logs and Prometheus metrics:

- **Logs**: JSON-formatted, with caller info, stack traces on errors, and configurable sampling
- **Metrics**: HTTP request duration histograms, event processing latency, queue depths, error rates, and goroutine counts
- **Tracing**: Events carry `correlation_id` and `causation_id` for distributed trace correlation

## Development

```bash
# Run tests with race detection
make test

# Run linter
make lint

# Generate protobuf code
make proto-gen

# Install dev tools
make tools
```

## License

MIT
