# KairoLog

KairoLog is a Kafka-inspired distributed commit log project written in Go.

The current focus is the single-node broker and storage foundation: topics, partitions, append-only logs, segment files, index files, offset-based fetching, segment rotation, basic crash recovery, consumer offset commits, consumer group assignment, consumer group membership, heartbeat tracking, stale member detection, stale member removal, group rebalance calculation, and cleanup-and-rebalance flow.

## Current Features

* HTTP broker server
* Health check endpoint (`GET /health`)
* Topic creation endpoint (`POST /topics`)
* Topic listing endpoint (`GET /topics`)
* Topic-aware produce endpoint (`POST /produce`)
* Topic/partition-aware fetch endpoint (`GET /fetch`)
* Consumer offset commit endpoint (`POST /offsets/commit`)
* Consumer offset lookup endpoint (`GET /offsets`)
* Consumer group assignment endpoint (`POST /groups/assign`)
* Consumer group rebalance endpoint (`POST /groups/rebalance`)
* Consumer group cleanup-and-rebalance endpoint (`POST /groups/cleanup-and-rebalance`)
* Consumer group join endpoint (`POST /groups/join`)
* Consumer group leave endpoint (`POST /groups/leave`)
* Consumer group heartbeat endpoint (`POST /groups/heartbeat`)
* Consumer group members endpoint (`GET /groups/members`)
* Stale group members endpoint (`GET /groups/stale`)
* Stale group member removal endpoint (`POST /groups/remove-stale`)
* In-memory log component
* File-based storage component
* Offset-aware records
* Segment file abstraction
* Index file abstraction with real byte positions
* Index-backed reads
* Partition log abstraction
* Basic segment rotation
* Multiple segment/index pairs per partition
* Reopen support for rotated segment/index pairs
* Basic crash recovery for partition logs
* Missing index-file rebuild from existing segment logs
* Recovery of offset-to-byte-position mappings
* Consumer offset store
* Persistent consumer offset commits
* Consumer group assignment engine
* Deterministic balanced partition assignment
* Consumer group membership registry
* Join/leave group membership lifecycle
* Group member heartbeat tracking
* Last-seen timestamp tracking for group members
* Internal stale group member detection
* HTTP stale group member lookup
* Internal stale group member removal
* HTTP stale group member removal
* HTTP group rebalance calculation
* HTTP cleanup-and-rebalance flow
* Topic manager
* Partition manager
* Topic partitions wired to partition logs
* Unit tests for log, storage, topic, server, segment, index, partition, consumer, and group packages

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
→ group membership registry
→ heartbeat tracking
→ stale member detection
→ stale member removal
→ group rebalance calculation
→ cleanup-and-rebalance flow
```

Each topic contains one or more partitions. Each partition is backed by a partition log. The partition log writes records into append-only segment files and stores offset-to-byte-position mappings in matching index files.

Reads can use the index to seek into the segment instead of scanning from the beginning.

Segment rotation creates new segment/index pairs when the active segment reaches the configured size limit.

If an index file is missing during partition-log startup, KairoLog can rebuild it by scanning the matching segment log file and restoring offset-to-byte-position mappings.

Consumer offsets are stored separately so a consumer group can remember how far it has processed a topic partition.

The group assignment engine distributes topic partitions across consumer group members in a deterministic and balanced way. The HTTP broker exposes direct assignment through `POST /groups/assign`.

The group membership registry tracks members joining and leaving consumer groups. The HTTP broker exposes this through `POST /groups/join`, `POST /groups/leave`, and `GET /groups/members`.

The group registry tracks `LastSeen` timestamps for members. A member receives a timestamp when it joins, and the heartbeat endpoint updates that timestamp when the member is seen again.

The group registry can detect stale members by comparing each member’s `LastSeen` timestamp against a timeout window. The HTTP broker exposes detection through `GET /groups/stale`.

The group registry can remove stale members. The HTTP broker exposes stale-member removal through `POST /groups/remove-stale`.

The rebalance endpoint calculates topic partition assignments using the currently registered group members. It does not persist assignments yet.

The cleanup-and-rebalance endpoint removes stale members first, then calculates fresh assignments for the remaining active members.

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

Current group membership, heartbeat state, stale-member detection, stale-member removal state, rebalance assignments, and cleanup-and-rebalance assignments are in-memory only and are not persisted yet.

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

### Assign Topic Partitions to Provided Members

```http
POST /groups/assign
```

Example request:

```json
{
  "topic": "orders",
  "members": [
    {
      "id": "member-a"
    },
    {
      "id": "member-b"
    }
  ]
}
```

Example response:

```json
{
  "assignments": [
    {
      "member_id": "member-a",
      "topics": [
        {
          "topic": "orders",
          "partitions": [0, 1]
        }
      ]
    },
    {
      "member_id": "member-b",
      "topics": [
        {
          "topic": "orders",
          "partitions": [2, 3]
        }
      ]
    }
  ]
}
```

This endpoint assigns partitions using the member list provided in the request body.

### Rebalance Registered Consumer Group Members

```http
POST /groups/rebalance
```

Example request:

```json
{
  "group": "analytics-workers",
  "topic": "orders"
}
```

Example response:

```json
{
  "group": "analytics-workers",
  "assignments": [
    {
      "member_id": "member-a",
      "topics": [
        {
          "topic": "orders",
          "partitions": [0, 1]
        }
      ]
    },
    {
      "member_id": "member-b",
      "topics": [
        {
          "topic": "orders",
          "partitions": [2, 3]
        }
      ]
    }
  ]
}
```

The rebalance endpoint reads the currently registered members from the group registry, then calculates topic partition assignments for those members.

Current rebalance behavior:

```text
registered group members
→ sorted by member ID
→ topic partition count
→ deterministic balanced assignment
```

This endpoint calculates assignments only. It does not persist assignments, commit offsets, remove stale members, or trigger background rebalancing.

### Cleanup and Rebalance Consumer Group

```http
POST /groups/cleanup-and-rebalance
```

Example request:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "timeout_ms": 300000
}
```

Example response:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "timeout_ms": 300000,
  "removed_members": [
    {
      "id": "member-a"
    }
  ],
  "assignments": [
    {
      "member_id": "member-b",
      "topics": [
        {
          "topic": "orders",
          "partitions": [0, 1, 2, 3]
        }
      ]
    }
  ]
}
```

The cleanup-and-rebalance endpoint performs two operations in one request:

```text
remove stale members
→ get remaining active members
→ calculate fresh topic partition assignments
```

A member is removed when:

```text
now - LastSeen > timeout
```

If all members are stale and removed, the endpoint returns `400 Bad Request` because there are no remaining active members to receive assignments.

This endpoint does not persist assignments, commit offsets, expose `LastSeen`, or run as a background cleanup process.

### Join Consumer Group

```http
POST /groups/join
```

Example request:

```json
{
  "group": "analytics-workers",
  "member_id": "member-a"
}
```

Example response:

```json
{
  "status": "joined"
}
```

### Leave Consumer Group

```http
POST /groups/leave
```

Example request:

```json
{
  "group": "analytics-workers",
  "member_id": "member-a"
}
```

Example response:

```json
{
  "status": "left"
}
```

### Record Consumer Group Heartbeat

```http
POST /groups/heartbeat
```

Example request:

```json
{
  "group": "analytics-workers",
  "member_id": "member-a"
}
```

Example response:

```json
{
  "status": "heartbeat recorded"
}
```

The heartbeat endpoint records that a group member was recently seen. Internally, it updates the member’s `LastSeen` timestamp. If the member does not already exist in the group, the heartbeat creates the member entry.

### List Consumer Group Members

```http
GET /groups/members?group=analytics-workers
```

Example response:

```json
{
  "group": "analytics-workers",
  "members": [
    {
      "id": "member-a"
    },
    {
      "id": "member-b"
    }
  ]
}
```

The members endpoint currently returns member IDs only. `LastSeen` is tracked internally but is not exposed in this response yet.

### List Stale Consumer Group Members

```http
GET /groups/stale?group=analytics-workers&timeout_ms=300000
```

Example response:

```json
{
  "group": "analytics-workers",
  "timeout_ms": 300000,
  "members": [
    {
      "id": "member-a"
    }
  ]
}
```

The stale members endpoint checks which members have not been seen within the requested timeout window.

A member is considered stale when:

```text
now - LastSeen > timeout
```

The endpoint currently returns member IDs only. `LastSeen` is still tracked internally but is not exposed in this response yet.

If the group does not exist, the endpoint returns an empty members array.

### Remove Stale Consumer Group Members

```http
POST /groups/remove-stale
```

Example request:

```json
{
  "group": "analytics-workers",
  "timeout_ms": 300000
}
```

Example response:

```json
{
  "group": "analytics-workers",
  "timeout_ms": 300000,
  "removed_members": [
    {
      "id": "member-a"
    }
  ]
}
```

The stale-member removal endpoint removes members that have not been seen within the requested timeout window.

A member is removed when:

```text
now - LastSeen > timeout
```

The endpoint currently returns removed member IDs only. `LastSeen` is still tracked internally but is not exposed in this response yet.

If the group does not exist, the endpoint returns an empty `removed_members` array.

This endpoint removes stale members from the in-memory group registry. It does not trigger automatic partition reassignment or rebalancing.

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

## Consumer Group Rebalance

The rebalance endpoint calculates topic partition assignments using the current registered members of a group.

Example:

```text
group: analytics-workers
topic: orders
registered members: member-b, member-a
partitions: 0, 1, 2, 3
```

The registry returns members sorted by ID:

```text
member-a
member-b
```

Result:

```text
member-a → partitions 0, 1
member-b → partitions 2, 3
```

Current rebalance limits:

```text
assignments are calculated only
assignments are not persisted
stale members are not removed inside /groups/rebalance
offsets are not committed during rebalance
background rebalance is not triggered
```

## Cleanup and Rebalance Flow

The cleanup-and-rebalance endpoint combines stale-member removal with fresh partition assignment.

Example before cleanup:

```text
group: analytics-workers
topic: orders
timeout: 5 minutes

member-a LastSeen: 11:54 → stale
member-b LastSeen: 11:59 → active
member-c LastSeen: 11:59 → active
```

Request at 12:00:

```http
POST /groups/cleanup-and-rebalance
```

Result:

```text
removed: member-a
remaining active members: member-b, member-c
new assignment:
member-b → partitions 0, 1
member-c → partitions 2, 3
```

Current cleanup-and-rebalance limits:

```text
assignments are calculated only
assignments are not persisted
offsets are not committed
LastSeen is not exposed in HTTP responses
cleanup runs only when the endpoint is called
there is no background cleanup loop yet
```

## Consumer Group Membership

The group membership registry tracks active members for each group.

Example:

```text
group: analytics-workers
members: member-a, member-b
```

Supported behavior:

```text
member joins group
member leaves group
member heartbeat is recorded
current members can be listed
stale members can be detected
stale members can be removed
registered active members can be rebalanced
stale members can be removed and remaining members rebalanced in one request
duplicate joins are idempotent
leaving a missing member is idempotent
heartbeat for a missing member creates the member
```

Membership is currently in-memory only. It is not persisted yet.

## Heartbeat Tracking

The group registry tracks when each group member was last seen.

Current behavior:

```text
Join(group, memberID)
→ adds the member
→ sets LastSeen for a new member

Heartbeat(group, memberID, now)
→ updates LastSeen for an existing member
→ adds the member if it is missing

Leave(group, memberID)
→ removes the member
→ removes its LastSeen timestamp
```

Heartbeat tracking is implemented inside the group registry and exposed through `POST /groups/heartbeat`.

Heartbeat tracking does not run automatic background cleanup yet.

## Stale Member Detection

The group registry can detect stale members using each member’s `LastSeen` timestamp.

Current behavior:

```text
StaleMembers(group, now, timeout)
→ checks all members in the group
→ returns members where now - LastSeen > timeout
→ returns stale members sorted by member ID
```

Example:

```text
now: 12:00
timeout: 5 minutes

member-a LastSeen: 11:54 → stale
member-b LastSeen: 11:57 → active
member-c LastSeen: 11:50 → stale
```

Result:

```text
member-a
member-c
```

Stale member detection is implemented inside the group registry and exposed through `GET /groups/stale`.

## Stale Member Removal

The group registry can remove stale members using the same timeout rule.

Current behavior:

```text
RemoveStaleMembers(group, now, timeout)
→ checks all members in the group
→ removes members where now - LastSeen > timeout
→ returns removed members sorted by member ID
→ keeps active members in the group
→ removes the group entry if all members are removed
```

Example:

```text
now: 12:00
timeout: 5 minutes

member-a LastSeen: 11:54 → removed
member-b LastSeen: 11:57 → kept
member-c LastSeen: 11:50 → removed
```

Result:

```text
removed: member-a, member-c
remaining: member-b
```

Stale member removal is implemented inside the group registry and exposed through `POST /groups/remove-stale`.

Stale member removal does not persist group state or trigger background rebalancing.

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

* HTTP broker foundation
* Topic and partition management
* Offset-aware append and fetch behavior
* Segment files
* Index files with real byte positions
* Index-backed reads
* Segment rotation
* Missing-index rebuild on recovery
* Consumer offset store
* Consumer offset commit and lookup endpoints
* Consumer group assignment engine
* Consumer group assignment endpoint
* Consumer group membership registry
* Consumer group membership endpoints
* Internal heartbeat tracking foundation
* Consumer group heartbeat endpoint
* Internal stale member detection
* Stale group members endpoint
* Internal stale member removal
* Stale group member removal endpoint
* Group rebalance endpoint
* Cleanup-and-rebalance endpoint

Still planned:

* Persistent rebalance assignment state
* Persistent group membership and heartbeat state
* Background stale-member cleanup loop
* Stronger crash recovery beyond missing-index rebuild
* CLI client
* Docker Compose demo
* Metrics and benchmarks
* Multi-broker replication
* Leader election / Raft-style coordination
* Final documentation and demo polish

## Project Goal

The goal is to build a sophisticated Kafka-inspired commit log system from scratch to demonstrate understanding of storage internals, broker design, distributed systems foundations, and fault-tolerant infrastructure.
