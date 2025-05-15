# Changelog

## [1.0.0] - 2025-05-15

### Added
- DAG-based task execution with parallel branch processing
- Conditional task execution with skip policies
- Webhook delivery system with HMAC signing and retry
- Webhook delivery history with retention policy
- Circuit breaker pattern for external service calls
- Distributed tracing with OpenTelemetry
- Rate limiting middleware (token bucket algorithm)
- CORS middleware with configurable origins
- Request timeout middleware with graceful handling
- Panic recovery middleware with structured logging
- Structured request logging with request ID propagation
- RBAC with role hierarchy (admin, manager, operator, viewer)
- JWT and API key authentication
- Prometheus metrics with custom collectors
- Redis caching layer with TTL management
- Docker multi-stage builds
- GitHub Actions CI/CD workflows
- Protobuf definitions for pipeline messages
- Comprehensive API documentation

### Changed
- Config system refactored with circuit breaker and DAG sections
- Pipeline engine optimized for large DAGs
- Worker pool now supports dynamic scaling

### Fixed
- Race condition in queue consumer shutdown
- Memory leak in scheduler recovery loop
- Deadlock in webhook batch processor
