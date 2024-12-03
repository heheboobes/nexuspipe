FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGET=api

RUN case "$TARGET" in \
      api)      CMD_PATH=./cmd/api ;; \
      worker)   CMD_PATH=./cmd/worker ;; \
      scheduler) CMD_PATH=./cmd/scheduler ;; \
      migrator) CMD_PATH=./cmd/migrator ;; \
      *)        echo "Unknown target: $TARGET"; exit 1 ;; \
    esac && \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w -X main.commitHash=$(git rev-parse --short HEAD 2>/dev/null || echo unknown) -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o /app/nexuspipe .

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /app/nexuspipe /nexuspipe
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

USER 65532:65532

EXPOSE 8080

ENTRYPOINT ["/nexuspipe"]
