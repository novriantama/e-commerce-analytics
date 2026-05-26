# Stage 1: Build binary
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Download dependencies
COPY jobs/producer/go.mod ./
RUN go mod download

# Copy source code and build
COPY jobs/producer/main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o producer .

# Stage 2: Run container
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/producer .

CMD ["./producer"]
