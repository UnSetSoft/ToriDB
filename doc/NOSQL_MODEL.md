# NoSQL & JSON Reference

**ToriDB** provides a rich set of NoSQL data structures and native JSON path support, allowing for flexible document storage alongside relational data.

## 1. Key-Value & TTL
### Basic Operations
- `SET key value`: Store a value.
- `GET key`: Retrieve a value.
- `INCR key` / `DECR key`: Atomic increments.

### Expiry (TTL)
- `SETEX key seconds value`: Set a value with a Time-To-Live.
- `TTL key`: Check remaining life of a key.

## 2. Complex Structures
### Lists
- `LPUSH tasks "Buy milk"`: Push to head.
- `LPOP tasks`: Pop from head.
- `LRange tasks 0 -1`: Get all items.

### Sorted Sets (ZSET)
Efficient for leaderboards and priority queues.
- `ZADD rank 100 "user1"`
- `ZRANGE rank 0 -1`: Get ordered members.

## 3. Native JSON Support
The engine parses and queries JSON documents natively using **JSON Path** syntax.

### JSON.SET
Store or update a part of a JSON document.
```text
JSON.SET profile $ '{"name": "Alice", "meta": {"role": "admin"}}'
JSON.SET profile $.meta.role '"superadmin"'
```

### JSON.GET
Retrieve specific paths.
```text
JSON.GET profile $.meta.role  -> "superadmin"
```

## 4. Other Structures
- **Hashes**: `HSET`, `HGET`, `HGETALL`.
- **Sets**: `SADD`, `SMEMBERS`.

---
[Back to Home](./README.md)
