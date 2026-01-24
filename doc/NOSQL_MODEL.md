# NoSQL & JSON Database Guide (⛩️)

ToriDB NoSQL mode provides flexible, schema-less storage with high-speed specialized data structures.

## 1. Key-Value & Atomicity

The simplest way to use ToriDB is as a concurrent key-value store.

- **SET/GET**: Basic assignment.
- **SETEX/TTL**: Automatic expiration.
- **INCR/DECR**: Atomic 64-bit counters, perfect for unique IDs or rate limiting.
- **DEL**: Atomic multi-key deletion.

---

## 2. Structured NoSQL (Collections)

ToriDB supports complex structures beyond simple strings.

### 2.1 Lists (Double-Ended Queues)
Atomic operations at both ends of a list. Use for timelines, task queues, or logging.
- `LPUSH / RPUSH`: Push elements.
- `LPOP / RPOP [count]`: Pop one or many elements.
- `LRANGE <start> <stop>`: Slice the list. support negative indices.

### 2.2 Sets (Unique Collections)
Unordered collection of unique strings.
- `SADD`: Add one or more members.
- `SMEMBERS`: Retrieve all members.

### 2.3 Sorted Sets (ZSET)
Collections where every member is associated with a **float score**.
- `ZADD <key> <score> <member>`: Add or update score.
- `ZRANGE <key> <start> <stop>`: Get items ordered by score.
- `ZSCORE <key> <member>`: Get current score.

### 2.4 Hashes (Objects/Dictionaries)
Maps between string fields and string values. efficient for representing objects.
- `HSET / HGET`: Field-level access.
- `HGETALL`: Fetch entire object.

---

## 3. Persistent JSON Store

Unlike simple Key-Value pairs, ToriDB understands the structure of JSON documents.

### 3.1 JSON.SET
**Syntax**: `JSON.SET <key> <path> <value>`

- Use `$` as the root path.
- Path syntax: `key->nested->field`.

```text
-- Initialize root
JSON.SET user:1 $ '{"active": true, "meta": {"login_count": 0}}'

-- Update deep field
JSON.SET user:1 meta->login_count 1
```

### 3.2 JSON.GET
**Syntax**: `JSON.GET <key> [path]`

- If path is omitted, returns the whole document.
- Returns stringified JSON for sub-elements.

---

## 4. Performance Tips
1. **Pipelining**: Batch multiple NoSQL commands in a single Transaction (`BEGIN...COMMIT`) to reduce network round-trips.
2. **Key Namespacing**: Use a colon `:` separator for logical grouping (e.g., `user:1001:profile`).

---
[Back to Home](../README.md)
