# KairoLog

KairoLog is a Kafka-inspired distributed commit log project written in Go.

The current focus is the single-node broker and storage foundation: topics, partitions, append-only logs, segment files, index files, offset-based fetching, segment rotation, basic crash recovery, consumer offset commits, consumer group assignment, consumer group membership, heartbeat tracking, stale member detection, stale member removal, group rebalance calculation, cleanup-and-rebalance flow, saved assignment lookup, saved assignment deletion, server-wired file-backed assignment persistence, and a file-backed group registry foundation.

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
* Consumer group saved assignment lookup endpoint (`GET /groups/assignments`)
* Consumer group saved assignment delete endpoint (`DELETE /groups/assignments`)
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
* Assignment store abstraction inside the server
* Internal in-memory group assignment store
* Internal file-backed group assignment store
* Server-wired file-backed assignment persistence
* Assignment save, lookup, delete, delete-group, and load operations
* Deep-copy protection for stored assignments
* JSONL-based assignment state persistence
* Automatic parent directory creation for assignment state files
* Assignment file reload support after restart
* Server assignment persistence at `data/group_assignments.log`
* Assignment persistence after `/groups/rebalance`
* Assignment persistence after `/groups/cleanup-and-rebalance`
* Assignment deletion persistence after `DELETE /groups/assignments`
* Saved assignment reload after server restart
* Internal file-backed group registry foundation
* File-backed group membership save, lookup, stale detection, stale removal, and load operations
* File-backed `Join`, `Heartbeat`, `Leave`, `Members`, `State`, `StaleMembers`, and `RemoveStaleMembers`
* JSONL-based group registry persistence
* Group membership reload support after restart
* LastSeen timestamp persistence foundation
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
→ assignment store interface
→ file-backed assignment store
→ saved assignment lookup
→ saved assignment deletion
```

```text
internal/group
→ assignment engine
→ in-memory assignment store
→ file-backed assignment store
→ in-memory membership registry
→ file-backed membership registry
```

Each topic contains one or more partitions. Each partition is backed by a partition log. The partition log writes records into append-only segment files and stores offset-to-byte-position mappings in matching index files.

Reads can use the index to seek into the segment instead of scanning from the beginning.

Segment rotation creates new segment/index pairs when the active segment reaches the configured size limit.

If an index file is missing during partition-log startup, KairoLog can rebuild it by scanning the matching segment log file and restoring offset-to-byte-position mappings.

Consumer offsets are stored separately so a consumer group can remember how far it has processed a topic partition.

The group assignment engine distributes topic partitions across consumer group members in a deterministic and balanced way. The HTTP broker exposes direct assignment through `POST /groups/assign`.

The in-memory group membership registry tracks members joining and leaving consumer groups. The HTTP broker exposes this through `POST /groups/join`, `POST /groups/leave`, and `GET /groups/members`.

The group registry tracks `LastSeen` timestamps for members. A member receives a timestamp when it joins, and the heartbeat endpoint updates that timestamp when the member is seen again.

The group registry can detect stale members by comparing each member’s `LastSeen` timestamp against a timeout window. The HTTP broker exposes detection through `GET /groups/stale`.

The group registry can remove stale members. The HTTP broker exposes stale-member removal through `POST /groups/remove-stale`.

The rebalance endpoint calculates topic partition assignments using the currently registered group members, then saves the latest assignment state into the server’s file-backed assignment store.

The cleanup-and-rebalance endpoint removes stale members first, calculates fresh assignments for the remaining active members, then saves the latest assignment state into the server’s file-backed assignment store.

The saved assignment lookup endpoint reads the latest stored assignment state for a group/topic pair from the server assignment store.

The saved assignment delete endpoint removes the saved assignment state for a group/topic pair without deleting group membership, offsets, or topic data.

The server now wires `AssignmentFileStore` as its default assignment store. Assignment state is saved to disk at `data/group_assignments.log` and loaded again when the server starts.

The new `RegistryFileStore` exists inside `internal/group`. It can persist group membership and `LastSeen` timestamps to disk, but it is not wired into the HTTP server yet.

## Storage Layout

KairoLog stores data under the `data` directory.

Example:

```text
data/
├── consumer_offsets.log
├── group_assignments.log
├── group_registry.log
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

The group assignment file stores latest assignment state for group/topic pairs.

The group registry file can store group membership and `LastSeen` heartbeat state. This file-backed registry is implemented and tested, but it is not wired into server startup yet.

Current server assignment state and consumer offsets are persisted. Current server group membership and heartbeat state are still in-memory until `RegistryFileStore` is wired into the server.

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

This endpoint assigns partitions using the member list provided in the request body. It does not save assignments into the server assignment store.

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

The rebalance endpoint reads the currently registered members from the group registry, calculates topic partition assignments for those members, and saves the latest assignment result into the server assignment store.

Current rebalance behavior:

```text
registered group members
→ sorted by member ID
→ topic partition count
→ deterministic balanced assignment
→ save latest assignment state by group/topic
→ persist assignment state to data/group_assignments.log
```

This endpoint does not commit offsets, remove stale members, or trigger background rebalancing.

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

The cleanup-and-rebalance endpoint performs three operations in one request:

```text
remove stale members
→ get remaining active members
→ calculate fresh topic partition assignments
→ save latest assignment state by group/topic
→ persist assignment state to data/group_assignments.log
```

A member is removed when:

```text
now - LastSeen > timeout
```

If all members are stale and removed, the endpoint returns `400 Bad Request` because there are no remaining active members to receive assignments.

This endpoint does not commit offsets, expose `LastSeen`, or run as a background cleanup process.

### Get Saved Consumer Group Assignments

```http
GET /groups/assignments?group=analytics-workers&topic=orders
```

Example response when found:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "found": true,
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

Example response when not found:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "found": false,
  "assignments": []
}
```

This endpoint reads the latest saved assignment result from the server assignment store.

It returns `found: true` when assignments exist for the requested group/topic pair.

It returns `found: false` when no assignment has been saved for that group/topic pair.

Because the server now uses the file-backed assignment store, saved assignment state can be loaded again after server restart.

This endpoint does not calculate a new assignment, remove stale members, commit offsets, or mutate stored state.

### Delete Saved Consumer Group Assignments

```http
DELETE /groups/assignments?group=analytics-workers&topic=orders
```

Example response:

```json
{
  "status": "deleted",
  "group": "analytics-workers",
  "topic": "orders"
}
```

This endpoint deletes the saved assignment result for the requested group/topic pair.

Deleting a missing assignment is idempotent and still returns `200 OK`.

Because the server now uses the file-backed assignment store, assignment deletion is persisted to disk.

This endpoint does not delete group membership, consumer offsets, topic data, or all assignments for a group. It only removes the saved assignment state for the requested group/topic pair.

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

Currently, this endpoint uses the server’s in-memory group registry. File-backed registry support exists internally, but it is not wired into this endpoint yet.

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

Currently, this endpoint uses the server’s in-memory group registry. File-backed registry support exists internally, but it is not wired into this endpoint yet.

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

Currently, this endpoint uses the server’s in-memory group registry. File-backed registry support exists internally, but it is not wired into this endpoint yet.

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

Currently, this endpoint uses the server’s in-memory group registry. File-backed registry support exists internally, but it is not wired into this endpoint yet.

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

This endpoint removes stale members from the group registry. It does not trigger automatic partition reassignment or rebalancing.

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

The rebalance endpoint calculates topic partition assignments using the current registered members of a group and saves the result into the file-backed assignment store.

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

Saved internally as:

```text
analytics-workers/orders
→ latest assignment result
```

Persisted at:

```text
data/group_assignments.log
```

The saved result can be read through:

```http
GET /groups/assignments?group=analytics-workers&topic=orders
```

The saved result can be deleted through:

```http
DELETE /groups/assignments?group=analytics-workers&topic=orders
```

Current rebalance limits:

```text
server group membership is still in-memory
server heartbeat state is still in-memory
file-backed group registry exists but is not wired into server yet
stale members are not removed inside /groups/rebalance
offsets are not committed during rebalance
background rebalance is not triggered
```

## Cleanup and Rebalance Flow

The cleanup-and-rebalance endpoint combines stale-member removal with fresh partition assignment and saves the result into the file-backed assignment store.

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

Saved internally as:

```text
analytics-workers/orders
→ latest assignment result after cleanup
```

Persisted at:

```text
data/group_assignments.log
```

The saved result can be read through:

```http
GET /groups/assignments?group=analytics-workers&topic=orders
```

The saved result can be deleted through:

```http
DELETE /groups/assignments?group=analytics-workers&topic=orders
```

Current cleanup-and-rebalance limits:

```text
server group membership is still in-memory
server heartbeat state is still in-memory
file-backed group registry exists but is not wired into server yet
offsets are not committed
LastSeen is not exposed in HTTP responses
cleanup runs only when the endpoint is called
there is no background cleanup loop yet
```

## In-Memory Assignment Store

The in-memory assignment store keeps the latest assignment result for a group/topic pair during the current process lifetime.

Internal API:

```text
NewAssignmentStore()
Save(group, topic, assignments)
Get(group, topic)
Delete(group, topic)
DeleteGroup(group)
```

Assignment store behavior:

```text
Save → stores/replaces latest assignments for group/topic
Get → returns assignments and found=true when present
Get → returns found=false when missing
Delete → removes one group/topic assignment
DeleteGroup → removes all assignments for a group
```

The in-memory assignment store is concurrency-safe and uses an internal `sync.RWMutex`.

Assignments are deep-copied when saved and deep-copied again when fetched. This prevents outside mutation from corrupting store state.

The server can still use the in-memory store in tests because the server depends on an assignment store interface.

## File-Backed Assignment Store

The file-backed assignment store persists the latest assignment state for group/topic pairs to disk.

Internal API:

```text
NewAssignmentFileStore(path)
Save(group, topic, assignments)
Get(group, topic)
Delete(group, topic)
DeleteGroup(group)
Load()
```

File-backed store behavior:

```text
Save → stores/replaces latest assignments for group/topic and writes state to disk
Get → returns assignments and found=true when present
Get → returns found=false when missing
Delete → removes one group/topic assignment and writes updated state to disk
DeleteGroup → removes all assignments for one group and writes updated state to disk
Load → restores assignment state from disk
```

The file-backed assignment store uses a simple JSON Lines format.

Example record:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "assignments": [
    {
      "member_id": "member-a",
      "topics": [
        {
          "topic": "orders",
          "partitions": [0, 1]
        }
      ]
    }
  ]
}
```

The file-backed assignment store is concurrency-safe and uses an internal `sync.RWMutex`.

Assignments are deep-copied on save and get.

Parent directories are created automatically.

A missing assignment file is treated as empty state.

The HTTP server now uses the file-backed assignment store by default at:

```text
data/group_assignments.log
```

## In-Memory Group Registry

The in-memory group registry tracks consumer group membership and heartbeat state during the current process lifetime.

Internal API:

```text
NewRegistry()
Join(group, memberID)
Heartbeat(group, memberID, now)
Leave(group, memberID)
Members(group)
State(group)
StaleMembers(group, now, timeout)
RemoveStaleMembers(group, now, timeout)
```

Registry behavior:

```text
Join → adds member and sets LastSeen for new members
Heartbeat → updates LastSeen and creates missing member
Leave → removes member
Members → returns sorted group members
State → returns group name and sorted members
StaleMembers → returns stale members without removing them
RemoveStaleMembers → removes stale members and returns removed members
```

The HTTP server currently uses this in-memory registry.

## File-Backed Group Registry

The file-backed group registry persists group membership and `LastSeen` heartbeat state to disk.

Internal API:

```text
NewRegistryFileStore(path)
Join(group, memberID)
Heartbeat(group, memberID, now)
Leave(group, memberID)
Members(group)
State(group)
StaleMembers(group, now, timeout)
RemoveStaleMembers(group, now, timeout)
Load()
```

File-backed registry behavior:

```text
Join → adds member and persists registry state
Heartbeat → updates LastSeen and persists registry state
Leave → removes member and persists registry state
Members → returns sorted group members
State → returns group name and sorted members
StaleMembers → returns stale members without mutating state
RemoveStaleMembers → removes stale members and persists registry state
Load → restores group/member/LastSeen state from disk
```

The file-backed registry uses a simple JSON Lines format.

Example record:

```json
{
  "group": "analytics-workers",
  "member_id": "member-a",
  "last_seen": "2026-04-30T12:00:00Z"
}
```

The file-backed registry is concurrency-safe and uses an internal `sync.RWMutex`.

Parent directories are created automatically.

A missing registry file is treated as empty state.

Records are persisted deterministically by sorted group and sorted member ID.

Current file-backed registry limits:

```text
implemented and tested inside internal/group
not wired into the HTTP server yet
does not persist assignments
does not commit offsets
does not trigger rebalance by itself
does not expose LastSeen through HTTP responses yet
```

## Server Assignment Store Wiring

The server now depends on an internal assignment store interface instead of a concrete in-memory assignment store.

Server assignment store interface:

```text
Save(group, topic, assignments)
Get(group, topic)
Delete(group, topic)
```

Default server startup behavior:

```text
New()
→ create consumer offset store at data/consumer_offsets.log
→ create file-backed assignment store at data/group_assignments.log
→ load assignment state from disk
→ wire assignment store into server
```

This allows:

```text
POST /groups/rebalance
→ persist assignment state

POST /groups/cleanup-and-rebalance
→ persist assignment state

GET /groups/assignments
→ read persisted assignment state

DELETE /groups/assignments
→ persist assignment deletion
```

Restart behavior:

```text
server starts
→ data/group_assignments.log is loaded
→ saved assignment state becomes queryable again
```

Current server registry persistence status:

```text
RegistryFileStore exists internally
server still uses in-memory Registry
next block should wire RegistryFileStore into server
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
latest assignments can be stored internally by group/topic
latest saved assignments can be read through HTTP
latest saved assignments can be deleted through HTTP
saved assignments can survive server restart
file-backed group registry can persist membership internally
duplicate joins are idempotent
leaving a missing member is idempotent
heartbeat for a missing member creates the member
```

Server membership is currently in-memory only. File-backed membership persistence exists internally but is not wired into server startup yet.

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

File-backed heartbeat persistence exists in `RegistryFileStore`, but the HTTP server still uses the in-memory registry until the next wiring step.

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

The file-backed registry can persist stale-member removal internally, but the server has not been wired to use it yet.

## Running Tests

Run all tests:

```bash
go test ./...
```

Run server package tests only:

```bash
go test ./internal/server
```

Run group package tests only:

```bash
go test ./internal/group
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
* Internal in-memory assignment store
* Internal file-backed assignment store
* Assignment store interface inside server
* Server-wired file-backed assignment persistence
* Saved assignment lookup endpoint
* Saved assignment delete endpoint
* Assignment file load/reload support
* Assignment persistence tests
* Server restart persistence tests for assignment state
* Internal file-backed group registry
* File-backed group membership persistence tests
* File-backed heartbeat persistence tests
* File-backed stale-member removal persistence tests

Still planned:

* Wire file-backed group registry into the HTTP server
* Persist server group membership and heartbeat state
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
