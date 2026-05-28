# Stage 1: Build binary
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Download dependencies
COPY jobs/producer/go.mod ./
COPY jobs/producer/main.go ./
RUN go mod tidy

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o producer .

# Stage 2: Run container
FROM alpine:latest

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /root/

COPY --from=builder /app/producer .

CMD ["./producer"]
