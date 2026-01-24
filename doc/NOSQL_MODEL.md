# NoSQL & JSON Database Guide (⛩️)

ToriDB NoSQL mode provides flexible, schema-less storage with high-speed specialized data structures.

## 1. Key-Value & Atomicity

The fastest way to store data, utilizing lock-free concurrent hash maps.

- **SET / GET**: Primary operations. Value can be any string or JSON.
- **SETEX / TTL**: Automatic expiration with sub-millisecond precision.
- **DEL**: supports multiple keys in a single atomic operation.
- **INCR / DECR**: Atomic 64-bit integer counters. Handles overflow/underflow safely.

---

## 2. Advanced Data Structures

Access collections directly by key.

### 2.1 Lists (Deque)
Double-ended queues optimized for fast push/pop at both ends.
- `LPUSH / RPUSH`: Add elements.
- `LPOP / RPOP [count]`: remove elements.
- `LRANGE <start> <stop>`: Get a slice (e.g., `LRANGE mylist 0 -1` for all).

### 2.2 Sets
Unordered collection of unique strings. O(1) membership checks.
- `SADD`: Add members.
- `SMEMBERS`: Get all members.

### 2.3 Sorted Sets (ZSET)
Priority-ordered collections using **float scores**.
- `ZADD <key> <score> <member>`: Add or update a member's priority.
- `ZRANGE <key> <start> <stop>`: Get members ordered by score (ascending).
- `ZSCORE <key> <member>`: Check current rank.

### 2.4 Hashes
key-Field mapping, perfect for storing complex objects without stringifying the entire thing.
- `HSET / HGET`: Field-level operations.
- `HGETALL`: Returns the entire hash as an object/map.

---

## 3. Native JSON Documents

ToriDB understands JSON. You can modify parts of a document without re-writing the whole string.

### 3.1 JSON.SET
**Syntax**: `JSON.SET <key> <path> <value>`
- **Root**: Use `$` to set the entire object.
- **Paths**: Use `->` to traverse (e.g., `user:101 config->theme`).

### 3.2 JSON.GET
**Syntax**: `JSON.GET <key> [path]`
- returns stringified JSON segments based on the path provided.

---

## 4. Performance Guidelines
1. **Concurrency**: ToriDB uses `DashMap`, meaning multiple threads can read/write different keys without competition.
2. **Persistence**: NoSQL operations are logged to the AOF. Large JSON documents will increase AOF growth; consider periodic `REWRITEAOF`.

---
[Back to Document Index](../README.md)
