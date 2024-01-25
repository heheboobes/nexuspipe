# NexusPipe Architecture

## System Overview

NexusPipe is a distributed event processing pipeline built on Go, PostgreSQL, and RabbitMQ. It follows a microservices architecture with four main components.

## Components

### API Server
RESTful API gateway handling pipeline CRUD, event ingestion, and system management. Built with Gin framework.

### Worker Service
Background consumer that processes pipeline events from RabbitMQ queues. Implements configurable worker pools with graceful shutdown.

### Scheduler Service
Cron-based scheduler for periodic pipeline execution. Stores schedules in PostgreSQL and uses RabbitMQ for dispatching.

### Migrator
CLI tool for database schema migrations using golang-migrate.

## Data Flow

1. Events enter through API Server
2. API validates and publishes to RabbitMQ exchange
3. Worker consumes events and executes pipeline stages
4. Results are persisted to PostgreSQL
5. Scheduler triggers time-based pipelines

## Database Schema

- pipelines: Pipeline definitions with configuration
- events: Event records with status tracking
- tasks: Individual task executions within pipelines
- schedules: Cron schedule definitions

## Message Flow

API → Exchange → Queue → Worker → PostgreSQL
Scheduler → Exchange → Queue → Worker → PostgreSQL
