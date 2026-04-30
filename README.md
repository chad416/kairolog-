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
- Index file abstraction
- Partition log abstraction
- Topic manager
- Partition manager
- Topic partitions wired to partition logs
- Unit tests for log, storage, topic, server, segment, index, and partition packages