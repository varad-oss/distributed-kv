# Build stage
FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy dependency files first for better caching
COPY go.mod go.sum* ./
RUN go mod download 2>/dev/null || true

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /dkv ./cmd/server

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /dkv /app/dkv

# Create data directory
RUN mkdir -p /app/data

EXPOSE 8001 9001

ENTRYPOINT ["/app/dkv"]
