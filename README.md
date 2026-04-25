# KairoLog

KairoLog is a distributed commit log system inspired by Apache Kafka.

## Current Features

- HTTP broker server
- Health check endpoint (/health)
- Produce endpoint (POST /produce)
- Messages endpoint (GET /messages)
- In-memory log storage
- Unit tests for log component

## Project Structure
- cmd/kairolog-broker → entrypoint
- internal/server → HTTP server

## Status
Early development (Phase 1)