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