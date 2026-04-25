# KairoLog

KairoLog is a distributed commit log system inspired by Apache Kafka.

## Current Features

- HTTP broker server
- Health check endpoint (/health)
- Produce endpoint (POST /produce)
- Messages endpoint (GET /messages)
- In-memory log component
- File-based storage component
- HTTP server connected to persistent storage
- Unit tests for log component
- HTTP server tests
- Storage tests

## Project Structure
- cmd/kairolog-broker → entrypoint
- internal/server → HTTP server

## Status
Early development (Phase 1)