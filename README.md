# KairoLog

KairoLog is a Kafka-inspired distributed commit log project written in Go.

The current focus is the single-node storage and broker foundation: topics, partitions, append-only logs, segment files, index files, offset-based fetching, and basic crash recovery.

## Current Features

- HTTP broker server
- Health check endpoint (`GET /health`)
- Topic creation endpoint (`POST /topics`)
- Topic listing endpoint (`GET /topics`)
- Topic-aware produce endpoint (`POST /produce`)
- Topic/partition-aware fetch endpoint (`GET /fetch`)
- In-memory log component
- File-based storage component
- Offset-aware records
- Segment file abstraction
- Index file abstraction with real byte positions
- Index-backed reads
- Partition log abstraction
- Basic segment rotation
- Multiple segment/index pairs per partition
- Reopen support for existing rotated segments
- Basic crash recovery for partition logs
- Missing index-file rebuild from existing segment logs
- Recovery of offset-to-byte-position mappings
- Reopen support for rotated segment/index pairs
- Topic manager
- Partition manager
- Topic partitions wired to partition logs
- Unit tests for log, storage, topic, server, segment, index, and partition packages

## Current Architecture

```text
server
→ topic manager
→ topic
→ partition
→ partition log
→ segment files
→ index files
```

Each partition is backed by a partition log. The partition log writes records into append-only segment files and stores offset-to-byte-position mappings in matching index files.

Reads can use the index to seek into the segment instead of scanning from the beginning.

Segment rotation creates new segment/index pairs when the active segment reaches the configured size limit.

If an index file is missing during partition-log startup, KairoLog can rebuild it by scanning the matching segment log file and restoring offset-to-byte-position mappings.

## Storage Layout

KairoLog stores topic data under the `data` directory.

Example:

```text
data/
└── orders/
    └── partition-0/
        ├── 00000000000000000000.log
        ├── 00000000000000000000.index
        ├── 00000000000000000003.log
        └── 00000000000000000003.index
```

Segment files store records.

Index files store offset-to-byte-position mappings.

## API

### Health Check

```http
GET /health
```

Example response:

```json
{
  "status": "ok"
}
```

### Create Topic

```http
POST /topics
```

Example request:

```json
{
  "name": "orders",
  "partitions": 3
}
```

### List Topics

```http
GET /topics
```

Example response:

```json
{
  "topics": ["orders"]
}
```

### Produce Message

```http
POST /produce
```

Example request:

```json
{
  "topic": "orders",
  "partition": 0,
  "message": "created order 123"
}
```

Example response:

```json
{
  "status": "stored",
  "offset": 0
}
```

### Fetch Messages

```http
GET /fetch?topic=orders&partition=0&offset=0
```

Example response:

```json
{
  "records": [
    {
      "offset": 0,
      "message": "created order 123"
    }
  ]
}
```

## Running Tests

Run all tests:

```bash
go test ./...
```

If Windows Application Control or antivirus blocks temporary Go test executables, compile a package separately:

```bash
go test -c ./internal/server
```

## Current Status

The project currently has a strong single-node storage foundation.

Completed core areas:

- HTTP broker foundation
- Topic and partition management
- Offset-aware append and fetch behavior
- Segment files
- Index files
- Index-backed reads
- Segment rotation
- Missing-index rebuild on recovery

Still planned:

- Stronger crash recovery behavior
- Consumer offset commits
- Consumer groups
- CLI client
- Docker Compose demo
- Metrics and benchmarks
- Multi-broker replication
- Leader election / Raft-style coordination
- Final documentation and demo polish

## Project Goal

The goal is to build a sophisticated Kafka-inspired commit log system from scratch to demonstrate understanding of storage internals, broker design, distributed systems foundations, and fault-tolerant infrastructure.