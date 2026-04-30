# KairoLog

KairoLog is a Kafka-inspired distributed commit log project written in Go.

The current focus is the single-node broker and storage foundation: topics, partitions, append-only logs, segment files, index files, offset-based fetching, segment rotation, basic crash recovery, consumer offset commits, and consumer group assignment logic.

## Current Features

- HTTP broker server
- Health check endpoint (`GET /health`)
- Topic creation endpoint (`POST /topics`)
- Topic listing endpoint (`GET /topics`)
- Topic-aware produce endpoint (`POST /produce`)
- Topic/partition-aware fetch endpoint (`GET /fetch`)
- Consumer offset commit endpoint (`POST /offsets/commit`)
- Consumer offset lookup endpoint (`GET /offsets`)
- In-memory log component
- File-based storage component
- Offset-aware records
- Segment file abstraction
- Index file abstraction with real byte positions
- Index-backed reads
- Partition log abstraction
- Basic segment rotation
- Multiple segment/index pairs per partition
- Reopen support for rotated segment/index pairs
- Basic crash recovery for partition logs
- Missing index-file rebuild from existing segment logs
- Recovery of offset-to-byte-position mappings
- Consumer offset store
- Persistent consumer offset commits
- Consumer group assignment engine
- Deterministic balanced partition assignment
- Topic manager
- Partition manager
- Topic partitions wired to partition logs
- Unit tests for log, storage, topic, server, segment, index, partition, consumer, and group packages

## Current Architecture

```text
server
→ topic manager
→ topic
→ partition
→ partition log
→ segment files
→ index files
→ consumer offset store
→ group assignment engine
```

Each topic contains one or more partitions. Each partition is backed by a partition log. The partition log writes records into append-only segment files and stores offset-to-byte-position mappings in matching index files.

Reads can use the index to seek into the segment instead of scanning from the beginning.

Segment rotation creates new segment/index pairs when the active segment reaches the configured size limit.

If an index file is missing during partition-log startup, KairoLog can rebuild it by scanning the matching segment log file and restoring offset-to-byte-position mappings.

Consumer offsets are stored separately so a consumer group can remember how far it has processed a topic partition.

The group assignment engine distributes topic partitions across consumer group members in a deterministic and balanced way.

## Storage Layout

KairoLog stores topic data under the `data` directory.

Example:

```text
data/
├── consumer_offsets.log
└── orders/
    └── partition-0/
        ├── 00000000000000000000.log
        ├── 00000000000000000000.index
        ├── 00000000000000000003.log
        └── 00000000000000000003.index
```

Segment files store records.

Index files store offset-to-byte-position mappings.

The consumer offset file stores committed offsets for consumer groups.

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

### Commit Consumer Offset

```http
POST /offsets/commit
```

Example request:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "partition": 0,
  "offset": 42
}
```

Example response:

```json
{
  "status": "committed"
}
```

### Get Consumer Offset

```http
GET /offsets?group=analytics-workers&topic=orders&partition=0
```

Example response when found:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "partition": 0,
  "offset": 42,
  "found": true
}
```

Example response when not found:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "partition": 0,
  "offset": 0,
  "found": false
}
```

## Consumer Group Assignment

The group assignment engine distributes partitions across members.

Example:

```text
topic: orders
partitions: 0, 1, 2, 3
members: member-a, member-b
```

Result:

```text
member-a → partitions 0, 1
member-b → partitions 2, 3
```

The assignment is deterministic because members are sorted by ID before partitions are assigned.

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

The project currently has a strong single-node broker and storage foundation.

Completed core areas:

- HTTP broker foundation
- Topic and partition management
- Offset-aware append and fetch behavior
- Segment files
- Index files with real byte positions
- Index-backed reads
- Segment rotation
- Missing-index rebuild on recovery
- Consumer offset store
- Consumer offset commit and lookup endpoints
- Consumer group assignment engine

Still planned:

- HTTP endpoint for group assignment
- Stronger crash recovery beyond missing-index rebuild
- Consumer group balancing with membership lifecycle
- CLI client
- Docker Compose demo
- Metrics and benchmarks
- Multi-broker replication
- Leader election / Raft-style coordination
- Final documentation and demo polish

## Project Goal

The goal is to build a sophisticated Kafka-inspired commit log system from scratch to demonstrate understanding of storage internals, broker design, distributed systems foundations, and fault-tolerant infrastructure.