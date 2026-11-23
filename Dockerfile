# Multi-stage Dockerfile for ultra-lightweight Go runner
FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod ./
# COPY go.sum ./
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o master_harvester main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/master_harvester .

# Default HTTP server port
ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["./master_harvester"]
