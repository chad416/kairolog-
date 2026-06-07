# KairoLog

KairoLog is a Kafka-inspired distributed commit log project written in Go.

The current focus is the single-node broker and storage foundation: topics, partitions, append-only logs, segment files, index files, offset-based fetching, segment rotation, basic crash recovery, consumer offset commits, consumer group assignment, consumer group membership, heartbeat tracking, stale member detection, stale member removal, group rebalance calculation, cleanup-and-rebalance flow, saved assignment lookup, saved assignment deletion, server-wired file-backed assignment persistence, server-wired file-backed group registry persistence, group listing support, assignment topic listing support, background stale-member cleanup, and automatic assignment rebalance after background stale cleanup.

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
* Background stale-member cleanup loop
* Automatic saved-assignment rebalance after background stale cleanup
* Automatic saved-assignment deletion when all group members are stale
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
* Assignment store abstraction inside the server
* Internal in-memory group assignment store
* Internal file-backed group assignment store
* Server-wired file-backed assignment persistence
* Assignment save, lookup, delete, delete-group, load, and topic listing operations
* In-memory `AssignmentStore.Topics(group)` support
* File-backed `AssignmentFileStore.Topics(group)` support after load/restart
* Sorted saved-topic listing by consumer group
* Assignment topic listing reflects `Delete(group, topic)`
* Assignment topic listing reflects `DeleteGroup(group)`
* JSONL-based assignment state persistence
* Assignment persistence after `/groups/rebalance`
* Assignment persistence after `/groups/cleanup-and-rebalance`
* Assignment persistence after background stale cleanup and rebalance
* Assignment deletion persistence after `DELETE /groups/assignments`
* Saved assignment reload after server restart
* Group registry abstraction inside the server
* Internal in-memory group registry
* Internal file-backed group registry
* Server-wired file-backed group registry persistence
* File-backed `Join`, `Heartbeat`, `Leave`, `Members`, `Groups`, `State`, `StaleMembers`, and `RemoveStaleMembers`
* In-memory `Groups()` support for known consumer group discovery
* File-backed `Groups()` support after load/restart
* Sorted group name listing
* Empty/deleted group exclusion from group listings
* JSONL-based group registry persistence
* Group membership reload after server restart
* LastSeen heartbeat reload after server restart
* Join persistence after `/groups/join`
* Heartbeat persistence after `/groups/heartbeat`
* Leave deletion persistence after `/groups/leave`
* Stale-member removal persistence after `/groups/remove-stale`
* Rebalance support using persisted group members after restart
* Cleanup-and-rebalance support using persisted members and `LastSeen` after restart
* Automatic stale-member cleanup using registered group discovery
* Automatic rebalance using saved assignment topic discovery
* Automatic parent directory creation for persistent state files
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
→ group registry interface
→ file-backed group registry
→ group listing support
→ heartbeat tracking
→ stale member detection
→ manual stale member removal
→ background stale member cleanup
→ automatic saved-assignment rebalance
→ group rebalance calculation
→ cleanup-and-rebalance flow
→ assignment store interface
→ file-backed assignment store
→ assignment topic listing support
→ saved assignment lookup
→ saved assignment deletion
```

```text
internal/group
→ assignment engine
→ in-memory assignment store
→ file-backed assignment store
→ assignment topic listing support
→ in-memory membership registry
→ file-backed membership registry
→ group listing support
```

Each topic contains one or more partitions. Each partition is backed by a partition log. The partition log writes records into append-only segment files and stores offset-to-byte-position mappings in matching index files.

Reads can use the index to seek into the segment instead of scanning from the beginning.

Segment rotation creates new segment/index pairs when the active segment reaches the configured size limit.

If an index file is missing during partition-log startup, KairoLog can rebuild it by scanning the matching segment log file and restoring offset-to-byte-position mappings.

Consumer offsets are stored separately so a consumer group can remember how far it has processed a topic partition.

The group assignment engine distributes topic partitions across consumer group members in a deterministic and balanced way. The HTTP broker exposes direct assignment through `POST /groups/assign`.

The server uses a file-backed group registry as the default registry. Group membership and `LastSeen` heartbeat state are saved to disk at `data/group_registry.log` and loaded again when the server starts.

The group registry tracks members joining and leaving consumer groups. The HTTP broker exposes this through `POST /groups/join`, `POST /groups/leave`, and `GET /groups/members`.

The group registry tracks `LastSeen` timestamps for members. A member receives a timestamp when it joins, and the heartbeat endpoint updates that timestamp when the member is seen again.

The group registry can list known group names internally through `Groups()`. This is used by the background stale-member cleanup loop to discover all consumer groups.

The assignment store can list saved topics for a consumer group through `Topics(group)`. This is used by the background cleanup-and-rebalance logic to discover which saved group/topic assignments need recalculation after stale members are removed.

The group registry can detect stale members by comparing each member’s `LastSeen` timestamp against a timeout window. The HTTP broker exposes detection through `GET /groups/stale`.

The group registry can remove stale members manually through `POST /groups/remove-stale`.

The server also runs a background stale-member cleanup loop from the production `Start()` path. This loop periodically discovers all groups, removes stale members automatically, and then updates saved assignment state for affected groups.

When background cleanup removes stale members from a group, the server checks saved topics for that group. For each saved topic that still exists in the topic manager, it recalculates assignments using the remaining active group members and saves the updated assignment state.

If all members in a group are removed as stale, the server deletes saved assignments for that group’s saved topics. This prevents saved assignment state from pointing only to dead members.

The rebalance endpoint calculates topic partition assignments using the currently registered group members, then saves the latest assignment state into the server’s file-backed assignment store.

The cleanup-and-rebalance endpoint removes stale members first, calculates fresh assignments for the remaining active members, then saves the latest assignment state into the server’s file-backed assignment store.

The saved assignment lookup endpoint reads the latest stored assignment state for a group/topic pair from the server assignment store.

The saved assignment delete endpoint removes the saved assignment state for a group/topic pair without deleting group membership, offsets, or topic data.

The server wires `AssignmentFileStore` as its default assignment store. Assignment state is saved to disk at `data/group_assignments.log` and loaded again when the server starts.

The server wires `RegistryFileStore` as its default group registry. Membership and heartbeat state are saved to disk at `data/group_registry.log` and loaded again when the server starts.

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

The group registry file stores group membership and `LastSeen` heartbeat state.

Current persisted server state:

```text
consumer offsets → data/consumer_offsets.log
group assignments → data/group_assignments.log
group registry / heartbeat state → data/group_registry.log
```

## Manual Demo Flow

This demo uses PowerShell and assumes the broker is running locally on port `8080`.

For repeatable results, run the demo from a clean `data/` directory or use fresh group names. Reusing old group names can show older persisted state from previous demo runs.

### 1. Start the broker

From the project root:

```powershell
go run ./cmd/kairolog-broker
```

Open a second PowerShell window for HTTP requests.

Set the base URL:

```powershell
$base = "http://localhost:8080"
```

### 2. Check broker health

```powershell
Invoke-RestMethod -Method Get "$base/health"
```

Expected response:

```json
{
  "status": "ok"
}
```

### 3. Create a topic with four partitions

```powershell
Invoke-RestMethod -Method Post "$base/topics" `
  -ContentType "application/json" `
  -Body '{"name":"orders","partitions":4}'
```

List topics:

```powershell
Invoke-RestMethod -Method Get "$base/topics"
```

Expected response:

```json
{
  "topics": ["orders"]
}
```

### 4. Join two consumer group members

```powershell
Invoke-RestMethod -Method Post "$base/groups/join" `
  -ContentType "application/json" `
  -Body '{"group":"analytics-workers","member_id":"member-a"}'
```

```powershell
Invoke-RestMethod -Method Post "$base/groups/join" `
  -ContentType "application/json" `
  -Body '{"group":"analytics-workers","member_id":"member-b"}'
```

Verify group members:

```powershell
Invoke-RestMethod -Method Get "$base/groups/members?group=analytics-workers" |
  ConvertTo-Json -Depth 10
```

Expected response:

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

### 5. Rebalance the group

```powershell
Invoke-RestMethod -Method Post "$base/groups/rebalance" `
  -ContentType "application/json" `
  -Body '{"group":"analytics-workers","topic":"orders"}' |
  ConvertTo-Json -Depth 10
```

Expected assignment shape:

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

### 6. View saved assignments

```powershell
Invoke-RestMethod -Method Get "$base/groups/assignments?group=analytics-workers&topic=orders" |
  ConvertTo-Json -Depth 10
```

Expected result:

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

### 7. Demonstrate stale cleanup and reassignment

The production background cleanup loop uses the default stale timeout of `5 minutes`. For a quick manual demo, use the explicit cleanup-and-rebalance endpoint with a short timeout.

Wait long enough for `member-a` to become stale under a short timeout:

```powershell
Start-Sleep -Seconds 35
```

Heartbeat only `member-b`:

```powershell
Invoke-RestMethod -Method Post "$base/groups/heartbeat" `
  -ContentType "application/json" `
  -Body '{"group":"analytics-workers","member_id":"member-b"}'
```

Run cleanup-and-rebalance with a `30000 ms` timeout:

```powershell
Invoke-RestMethod -Method Post "$base/groups/cleanup-and-rebalance" `
  -ContentType "application/json" `
  -Body '{"group":"analytics-workers","topic":"orders","timeout_ms":30000}' |
  ConvertTo-Json -Depth 10
```

Expected result:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "timeout_ms": 30000,
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

Verify saved assignments were updated:

```powershell
Invoke-RestMethod -Method Get "$base/groups/assignments?group=analytics-workers&topic=orders" |
  ConvertTo-Json -Depth 10
```

Expected saved assignment:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "found": true,
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

### 8. Produce records

```powershell
Invoke-RestMethod -Method Post "$base/produce" `
  -ContentType "application/json" `
  -Body '{"topic":"orders","partition":0,"message":"created order 123"}'
```

```powershell
Invoke-RestMethod -Method Post "$base/produce" `
  -ContentType "application/json" `
  -Body '{"topic":"orders","partition":0,"message":"created order 456"}'
```

Expected shape:

```json
{
  "status": "stored",
  "offset": 0
}
```

The second message should return offset `1`.

### 9. Fetch records

```powershell
Invoke-RestMethod -Method Get "$base/fetch?topic=orders&partition=0&offset=0" |
  ConvertTo-Json -Depth 10
```

Expected response:

```json
{
  "records": [
    {
      "offset": 0,
      "message": "created order 123"
    },
    {
      "offset": 1,
      "message": "created order 456"
    }
  ]
}
```

Fetch from offset `1`:

```powershell
Invoke-RestMethod -Method Get "$base/fetch?topic=orders&partition=0&offset=1" |
  ConvertTo-Json -Depth 10
```

Expected response:

```json
{
  "records": [
    {
      "offset": 1,
      "message": "created order 456"
    }
  ]
}
```

### 10. Commit and read consumer offset

Commit offset `2` for the consumer group:

```powershell
Invoke-RestMethod -Method Post "$base/offsets/commit" `
  -ContentType "application/json" `
  -Body '{"group":"analytics-workers","topic":"orders","partition":0,"offset":2}'
```

Expected response:

```json
{
  "status": "committed"
}
```

Read the committed offset:

```powershell
Invoke-RestMethod -Method Get "$base/offsets?group=analytics-workers&topic=orders&partition=0"
```

Expected response:

```json
{
  "group": "analytics-workers",
  "topic": "orders",
  "partition": 0,
  "offset": 2,
  "found": true
}
```

### 11. Stop and restart persistence check

Stop the broker with `Ctrl + C`.

Start it again:

```powershell
go run ./cmd/kairolog-broker
```

Check saved assignments:

```powershell
Invoke-RestMethod -Method Get "$base/groups/assignments?group=analytics-workers&topic=orders" |
  ConvertTo-Json -Depth 10
```

Check committed offset:

```powershell
Invoke-RestMethod -Method Get "$base/offsets?group=analytics-workers&topic=orders&partition=0"
```

Expected: assignment state and consumer offset state should still be available because they are persisted under `data/`.

Note: saved assignments can be automatically deleted by the background cleanup loop if every member in the group becomes stale. For a clean persistence check, restart and verify shortly after creating/rebalancing the group, or keep at least one member active with heartbeats.

Note: topic metadata persistence is not fully implemented yet. If topic metadata is not restored after restart, topic-dependent operations may require recreating the topic first. Assignment and offset state still persist separately.

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

Because the server uses the file-backed registry, rebalance can use persisted group members after server restart.

This endpoint does not commit offsets or remove stale members.

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

Because the server uses the file-backed registry, cleanup-and-rebalance can use persisted members and persisted `LastSeen` timestamps after restart.

If all members are stale and removed, the endpoint returns `400 Bad Request` because there are no remaining active members to receive assignments.

This endpoint does not commit offsets or expose `LastSeen`.

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

Because the server uses the file-backed assignment store, saved assignment state can be loaded again after server restart.

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

Because the server uses the file-backed assignment store, assignment deletion is persisted to disk.

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

This endpoint records a member in the file-backed group registry.

Join behavior:

```text
trim group and member ID
validate non-empty group and member ID
add member if missing
set LastSeen for a new member
persist registry state to data/group_registry.log
```

Duplicate joins are idempotent.

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

This endpoint removes a member from the file-backed group registry.

Leave behavior:

```text
trim group and member ID
validate non-empty group and member ID
remove member if present
persist registry state to data/group_registry.log
```

Leaving a missing member is idempotent.

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

Heartbeat behavior:

```text
trim group and member ID
validate non-empty group and member ID
set LastSeen to current server time
create member if missing
persist registry state to data/group_registry.log
```

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

The members endpoint reads from the server’s file-backed group registry.

The members endpoint currently returns member IDs only. `LastSeen` is persisted internally but is not exposed in this response yet.

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

The endpoint reads `LastSeen` state from the file-backed group registry.

The endpoint currently returns member IDs only. `LastSeen` is persisted internally but is not exposed in this response yet.

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

This endpoint removes stale members from the file-backed group registry and persists the updated registry state to disk.

This endpoint does not trigger automatic partition reassignment or rebalancing. Automatic rebalance happens only inside the background cleanup loop and the explicit cleanup-and-rebalance endpoint.

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
stale members are not removed inside /groups/rebalance
offsets are not committed during rebalance
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

The registry update is also persisted at:

```text
data/group_registry.log
```

The saved assignment result can be read through:

```http
GET /groups/assignments?group=analytics-workers&topic=orders
```

The saved assignment result can be deleted through:

```http
DELETE /groups/assignments?group=analytics-workers&topic=orders
```

Current cleanup-and-rebalance limits:

```text
offsets are not committed
LastSeen is not exposed in HTTP responses
cleanup-and-rebalance runs only when the endpoint is called
```

## In-Memory Assignment Store

The in-memory assignment store keeps the latest assignment result for a group/topic pair during the current process lifetime.

Internal API:

```text
NewAssignmentStore()
Save(group, topic, assignments)
Get(group, topic)
Topics(group)
Delete(group, topic)
DeleteGroup(group)
```

In-memory assignment store behavior:

```text
Save → stores/replaces latest assignments for group/topic
Get → returns assignments and found=true when present
Get → returns found=false when missing
Topics → returns sorted topic names with saved assignments for one group
Delete → removes one group/topic assignment
DeleteGroup → removes all assignments for one group
```

`Topics(group)` returns an empty slice when a group has no saved assignments.

`Topics(group)` validates that the group name is not empty.

The server can use the in-memory assignment store in tests because the server depends on an assignment store interface.

## File-Backed Assignment Store

The file-backed assignment store persists the latest assignment state for group/topic pairs to disk.

Internal API:

```text
NewAssignmentFileStore(path)
Save(group, topic, assignments)
Get(group, topic)
Topics(group)
Delete(group, topic)
DeleteGroup(group)
Load()
```

File-backed store behavior:

```text
Save → stores/replaces latest assignments for group/topic and writes state to disk
Get → returns assignments and found=true when present
Get → returns found=false when missing
Topics → returns sorted topic names with saved assignments for one group
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

The HTTP server uses the file-backed assignment store by default at:

```text
data/group_assignments.log
```

`Topics(group)` works after `Load()` and reflects persisted deletion after restart.

## Assignment Topic Listing Support

Both assignment store implementations support internal topic listing:

```text
AssignmentStore.Topics(group)
AssignmentFileStore.Topics(group)
```

Behavior:

```text
returns all saved topic names for one group
returns topic names sorted alphabetically
returns empty slice when the group has no saved assignments
returns only topics for the requested group
excludes deleted group/topic assignments
reflects Delete(group, topic)
reflects DeleteGroup(group)
works after AssignmentFileStore.Load()
rejects empty group
```

This is used by automatic rebalance after background stale-member cleanup.

The automatic background flow needs this because stale cleanup discovers groups first. To recalculate saved assignments, the server also needs to know which topics each affected group has saved assignment state for.

## In-Memory Group Registry

The in-memory group registry tracks consumer group membership and heartbeat state during the current process lifetime.

Internal API:

```text
NewRegistry()
Join(group, memberID)
Heartbeat(group, memberID, now)
Leave(group, memberID)
Members(group)
Groups()
State(group)
StaleMembers(group, now, timeout)
RemoveStaleMembers(group, now, timeout)
```

In-memory registry behavior:

```text
Join → adds member and sets LastSeen for new members
Heartbeat → updates LastSeen and creates missing members
Leave → removes a member
Members → returns sorted members for one group
Groups → returns sorted known group names
State → returns group name and sorted members
StaleMembers → returns stale members without mutating state
RemoveStaleMembers → removes stale members
```

`Groups()` returns sorted group names and excludes empty/deleted groups.

The server can still use the in-memory registry in tests because the server depends on a group registry interface.

## File-Backed Group Registry

The file-backed group registry persists group membership and `LastSeen` heartbeat state to disk.

Internal API:

```text
NewRegistryFileStore(path)
Join(group, memberID)
Heartbeat(group, memberID, now)
Leave(group, memberID)
Members(group)
Groups()
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
Groups → returns sorted known group names
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

The HTTP server uses the file-backed registry by default at:

```text
data/group_registry.log
```

Records are persisted deterministically by sorted group and sorted member ID.

`Groups()` works after `Load()` and reflects persisted deletion after restart.

Current file-backed registry limits:

```text
does not expose LastSeen through HTTP responses yet
does not expose Groups through HTTP yet
does not commit offsets
```

## Group Listing Support

Both registry implementations support internal group listing:

```text
Registry.Groups()
RegistryFileStore.Groups()
```

Behavior:

```text
returns all known group names
returns group names sorted alphabetically
returns empty slice when no groups exist
excludes groups with no members
reflects Leave deletion
reflects RemoveStaleMembers deletion
works after RegistryFileStore.Load()
```

This is used by the background stale-member cleanup loop:

```text
every interval
→ registry.Groups()
→ for each group
→ registry.RemoveStaleMembers(group, now, timeout)
```

## Background Stale-Member Cleanup and Automatic Rebalance

The server has a background stale-member cleanup and rebalance loop.

Purpose:

```text
periodically discover all consumer groups
remove stale members automatically
persist the registry cleanup result
update saved assignment state for affected groups
delete saved assignment state when all members are gone
```

Default values:

```text
defaultStaleCleanupInterval = 1 minute
defaultStaleMemberTimeout = 5 minutes
```

One cleanup-and-rebalance pass:

```text
cleanupStaleMembersAndRebalanceOnce(...)
→ validate registry, assignment store, topic manager, assigner, now, and timeout
→ groups = registry.Groups()
→ for each group:
   → removedMembers = registry.RemoveStaleMembers(group, now, timeout)
   → if no members were removed:
      → skip assignment work
   → if members were removed:
      → topics = assignmentStore.Topics(group)
      → activeMembers = registry.Members(group)
      → if no active members remain:
         → delete saved assignments for each saved topic
      → if active members remain:
         → for each saved topic:
            → check topic exists in topic manager
            → recalculate assignments
            → save updated assignment state
```

Background loop:

```text
startStaleMemberCleanup(...)
→ create ticker
→ on each tick:
   → run cleanupStaleMembersAndRebalanceOnce(...)
→ ignore cleanup errors and continue running
→ stop when context is cancelled
```

Production startup behavior:

```text
Start()
→ create configured server
→ create cancellable context
→ register cancellation on server shutdown
→ start background stale-member cleanup and rebalance
→ run ListenAndServe
```

Test-safe behavior:

```text
New()
→ creates server dependencies
→ does not start background goroutines
```

This prevents tests that call `New()` from leaking cleanup goroutines.

Automatic rebalance behavior:

```text
stale members removed
→ saved topics discovered through assignmentStore.Topics(group)
→ remaining active members loaded through registry.Members(group)
→ assignments recalculated with group.Assigner
→ updated assignments saved through assignmentStore.Save(...)
```

If all members are removed:

```text
stale members removed
→ no active members remain
→ saved assignments for the affected group/topics are deleted
```

If a saved assignment references a topic that is not currently loaded in the topic manager:

```text
topic missing
→ assignment is skipped
→ assignment state is not deleted
```

This is intentional because full topic metadata persistence is not implemented yet.

Current background cleanup and rebalance limits:

```text
does not commit offsets
does not expose new HTTP endpoints
does not expose LastSeen in HTTP responses
does not expose internal group/topic listing over HTTP
does not coordinate across multiple brokers
```

## Server Persistent State Wiring

Default server startup behavior:

```text
New()
→ create consumer offset store at data/consumer_offsets.log
→ create file-backed assignment store at data/group_assignments.log
→ load assignment state from disk
→ create file-backed group registry at data/group_registry.log
→ load registry state from disk
→ wire assignment store and registry into server
```

Production server startup behavior:

```text
Start()
→ build configured server
→ start background stale-member cleanup and rebalance loop
→ run HTTP server
```

This allows:

```text
POST /groups/rebalance
→ persist assignment state

POST /groups/cleanup-and-rebalance
→ persist assignment state and registry cleanup state

GET /groups/assignments
→ read persisted assignment state

DELETE /groups/assignments
→ persist assignment deletion

POST /groups/join
→ persist group membership

POST /groups/heartbeat
→ persist LastSeen

POST /groups/leave
→ persist member deletion

GET /groups/members
→ read persisted group membership

GET /groups/stale
→ evaluate persisted LastSeen timestamps

POST /groups/remove-stale
→ persist stale-member deletion

background cleanup and rebalance loop
→ automatically remove stale members
→ persist registry cleanup state
→ recalculate saved assignments for affected groups
→ persist updated assignment state
```

Restart behavior:

```text
server starts
→ data/group_assignments.log is loaded
→ data/group_registry.log is loaded
→ saved assignment state becomes queryable again
→ group membership becomes queryable again
→ LastSeen timestamps become available for stale-member logic
→ background stale cleanup and rebalance can continue from persisted registry and assignment state
```

## Consumer Group Membership

The group registry tracks active members for each group.

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
known group names can be listed internally
saved topics for a group can be listed internally
stale members can be detected
stale members can be removed manually
stale members can be removed automatically in the background
saved assignments can be rebalanced automatically after background cleanup
saved assignments can be deleted automatically when all members are stale
registered active members can be rebalanced manually
stale members can be removed and remaining members rebalanced in one request
latest assignments can be stored internally by group/topic
latest saved assignments can be read through HTTP
latest saved assignments can be deleted through HTTP
saved assignments can survive server restart
group membership can survive server restart
heartbeat LastSeen state can survive server restart
duplicate joins are idempotent
leaving a missing member is idempotent
heartbeat for a missing member creates the member
```

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

`LastSeen` is persisted but not exposed in HTTP responses yet.

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
→ persists updated registry state
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

Stale member removal is exposed manually through `POST /groups/remove-stale`.

Stale member removal also runs automatically through the background cleanup loop.

Automatic stale member removal can trigger saved assignment rebalance for affected groups.

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
* Consumer group membership endpoints
* Consumer group heartbeat endpoint
* Stale group members endpoint
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
* Assignment topic listing support
* File-backed assignment topic listing after load/restart
* Assignment persistence tests
* Server restart persistence tests for assignment state
* Internal in-memory group registry
* Internal file-backed group registry
* Group registry interface inside server
* Server-wired file-backed group registry persistence
* Registry group listing support
* File-backed group listing support after load/restart
* Background stale-member cleanup loop
* Automatic saved-assignment rebalance after background stale cleanup
* Automatic saved-assignment deletion when all group members are stale
* Deterministic one-pass stale cleanup and rebalance helper
* Background cleanup lifecycle tests
* Background rebalance tests
* File-backed group membership persistence tests
* File-backed heartbeat persistence tests
* File-backed stale-member removal persistence tests
* Server restart persistence tests for group membership and heartbeat state
* Manual demo flow for GitHub readers

Still planned for final polish of the strong single-node project:

* Run the manual demo flow once from PowerShell
* Confirm final README accuracy after demo
* Run final `go test ./...`
* Commit and push final documentation polish

Still planned for the larger full master roadmap / skyscraper:

* Stronger crash recovery beyond missing-index rebuild
* Topic metadata persistence
* CLI client
* Docker Compose demo
* Metrics and benchmarks
* Multi-broker replication
* Leader election / Raft-style coordination
* Final distributed-system-level polish

## Project Goal

The goal is to build a sophisticated Kafka-inspired commit log system from scratch to demonstrate understanding of storage internals, broker design, distributed systems foundations, and fault-tolerant infrastructure.

