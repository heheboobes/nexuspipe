.PHONY: build test lint migrate docker-build proto-gen clean run-api run-worker run-scheduler

APP_NAME    := nexuspipe
BUILD_DIR   := build
COMMIT_HASH := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME  := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS     := -ldflags "-X main.commitHash=$(COMMIT_HASH) -X main.buildTime=$(BUILD_TIME) -s -w"

# === Build ===
build: build-api build-worker build-scheduler build-migrator

build-api:
	@echo "Building API server..."
	go build $(LDFLAGS) -o $(BUILD_DIR)/nexuspipe-api ./cmd/api

build-worker:
	@echo "Building worker..."
	go build $(LDFLAGS) -o $(BUILD_DIR)/nexuspipe-worker ./cmd/worker

build-scheduler:
	@echo "Building scheduler..."
	go build $(LDFLAGS) -o $(BUILD_DIR)/nexuspipe-scheduler ./cmd/scheduler

build-migrator:
	@echo "Building migrator..."
	go build $(LDFLAGS) -o $(BUILD_DIR)/nexuspipe-migrator ./cmd/migrator

# === Test ===
test:
	@echo "Running tests..."
	go test -v -race -count=1 -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out | tail -1

test-cover:
	@echo "Opening coverage report..."
	go tool cover -html=coverage.out

# === Lint ===
lint:
	@echo "Running golangci-lint..."
	golangci-lint run ./... --timeout=5m --out-format=colored-line-number

lint-fix:
	@echo "Running golangci-lint with auto-fix..."
	golangci-lint run ./... --timeout=5m --fix

# === Migrations ===
migrate-up:
	@echo "Running migrations up..."
	@read -p "Enter DSN: " dsn; \
	migrate -path=./migrations -database "$$dsn" up

migrate-down:
	@echo "Running migrations down..."
	@read -p "Enter DSN: " dsn; \
	migrate -path=./migrations -database "$$dsn" down

migrate-create:
	@read -p "Enter migration name: " name; \
	migrate create -ext sql -dir ./migrations -seq "$$name"

# === Docker ===
docker-build:
	@echo "Building Docker images..."
	docker build -t nexuspipe/api:latest -f deployments/Dockerfile.api .
	docker build -t nexuspipe/worker:latest -f deployments/Dockerfile.worker .
	docker build -t nexuspipe/scheduler:latest -f deployments/Dockerfile.scheduler .
	docker build -t nexuspipe/migrator:latest -f deployments/Dockerfile.migrator .

docker-push:
	@echo "Pushing Docker images..."
	docker push nexuspipe/api:latest
	docker push nexuspipe/worker:latest
	docker push nexuspipe/scheduler:latest
	docker push nexuspipe/migrator:latest

# === Proto ===
proto-gen:
	@echo "Generating protobuf code..."
	protoc --proto_path=api/proto \
		--go_out=api/proto --go_opt=paths=source_relative \
		--go-grpc_out=api/proto --go-grpc_opt=paths=source_relative \
		api/proto/**/*.proto

# === Run ===
run-api:
	@echo "Starting API server..."
	go run ./cmd/api --config=config.yaml

run-worker:
	@echo "Starting worker..."
	go run ./cmd/worker --config=config.yaml

run-scheduler:
	@echo "Starting scheduler..."
	go run ./cmd/scheduler --config=config.yaml

# === Clean ===
clean:
	@echo "Cleaning build artifacts..."
	rm -rf $(BUILD_DIR)
	rm -f coverage.out
	rm -f profile.out

# === Tools ===
tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/golang-migrate/migrate/v4/cmd/migrate@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# === Help ===
help:
	@echo "NexusPipe Makefile"
	@echo "------------------"
	@echo "build           - Build all binaries"
	@echo "test            - Run all tests with race detection"
	@echo "lint            - Run golangci-lint"
	@echo "migrate-up      - Run database migrations up"
	@echo "migrate-down    - Rollback database migrations"
	@echo "docker-build    - Build all Docker images"
	@echo "proto-gen       - Generate protobuf code"
	@echo "run-api         - Start API server locally"
	@echo "run-worker      - Start background worker"
	@echo "run-scheduler   - Start cron scheduler"
	@echo "clean           - Remove build artifacts"
