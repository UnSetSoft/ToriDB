# ToriDB Node.js SDK Reference (⛩️)

The official Node.js client for **ToriDB**. Highly intuitive, multi-model, and optimized for low-latency workloads.

## 📦 Installation

```bash
npm install toridb
```

---

## 🔌 Connection & URI

ToriDB uses a **Unified Connection URI** to configure the client.

```javascript
const { ToriDB } = require('toridb');

// Format: db://[user]:[password][@|+][host]:[port][/[database]][?options]
const db = new ToriDB("db://admin:secret+127.0.0.1:8569/production?workers=20");

await db.connect();
```

### Methods
- `connect()`: Establishes socket connection and performs handshake (AUTH/USE).
- `disconnect()`: Closes the connection.
- `dbName(name)`: Programmatically switch database context. Sends `USE <name>` if already connected.
- `ping()`: Check server health. Returns `PONG`.
- `bgRewriteAof()`: Triggers background AOF compaction.
- `execute(...args)`: Send a raw RESP command array to the server.
- `query(string)`: Parse and send a raw string query.

---

## 🗝️ Key-Value & Atomicity

High-speed caching and counters.

| Method | Description |
| :--- | :--- |
| `set(key, val)` | Stores a value. Objects are auto-stringified. |
| `get(key)` | Retrieves a value. |
| `setEx(key, val, ttl)` | Stores a value with Expiration (seconds). |
| `ttl(key)` | Returns seconds remaining or error codes (-1, -2). |
| `del(...keys)` | Deletes one or more keys. Returns count. |
| `incr(key)` | Atomic increment. |
| `decr(key)` | Atomic decrement. |

---

## 📚 NoSQL Data Structures

Access specialized collections via sub-managers.

### Lists (`.list(key)`)
Use as queues, stacks, or timelines.
- `push(...vals)`: LPUSH (Head).
- `rpush(...vals)`: RPUSH (Tail).
- `pop([count])`: LPOP.
- `rpop([count])`: RPOP.
- `range(start, stop)`: Slice the list. Supports negative indices (0 -1 for all).

### Sets (`.setOf(key)`)
Unordered collections of unique strings.
- `add(...members)`: SADD.
- `members()`: SMEMBERS.

### Hashes (`.hash(key)`)
Optimized storage for objects/dictionaries.
- `set(field, val)`: HSET.
- `get(field)`: HGET.
- `all()`: HGETALL (returns object).

### Sorted Sets (`.sortedSet(key)`)
Priority queues and leaderboards.
- `add(score, member)`: ZADD.
- `range(start, stop)`: ZRANGE.
- `score(member)`: ZSCORE.

### JSON (`.json(key)`)
Deep document manipulation using the `->` operator.
- `set(path, val)`: JSON.SET. Use `$` for root.
- `get([path])`: JSON.GET. Defaults to root.

---

## 🏛️ Relational Modeling

ToriDB provides a powerful SQL layer with a MongoDB-like fluent interface.

### Blueprints & Models
Define a schema to get a typed model.

```javascript
const userBlueprint = new ToriDB.Blueprint({
    id: { type: 'INT', primary: true },
    email: { type: 'String', unique: true },
    profile: 'Object' // Maps to JSON
});

const User = db.model("users", userBlueprint);
```

**Model Methods:**
- `create(data)`: Validated insert.
- `find(filter)`: Starts a `QueryBuilder`.
- `findById(id)`: Fetches a single row.
- `update(filter, data)`: Update values matching criteria.
- `delete(filter)`: Remove rows matching criteria.
- `count()` / `sum(col)` / `avg(col)` / `max(col)` / `min(col)`: Helper methods for aggregate queries.
- `createIndex(idxName, col)`: Secondary indexing.
- `addColumn(col, type)` / `dropColumn(col)`: Schema migrations.

### Table API (`.table(name)`)
Access existing tables without defining a full Blueprint.
- Methods: `create`, `find`, `findById`, `update`, `delete`, `select`, `search`.

---

## 🔍 Query Builder

Chain methods to build complex SQL queries. Returned by `find()`, `select()`, or `search()`.

```javascript
const results = await User.find({ age: { $gt: 18 } })
    .select(["id", "email"])
    .join("profiles", "users.id", "profiles.user_id")
    .orderBy("created_at", "DESC")
    .limit(10)
    .offset(20)
    .having({ total_spent: { $gt: 100 } }) // Filter after aggregation
    .execute();
```

### Supported Operators
`$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$eq`, `$like`, `$in`, `$and`, `$or`.

### Vector Search
Perform high-speed similarity search for embeddings.
- `.search(column, vector, limit)`
- `.count()` / `.sum(col)` / `.avg(col)` / `.max(col)` / `.min(col)`

---

## 🛠️ System Manager (`.system`)

Administrative control over the server.

### ACL (`.system.acl`)
- `createUser(user, pass, rules)`: Rules format `["+get", "-delete"]`.
- `getUser(user)`
- `listUsers()`
- `deleteUser(user)`

### Cluster (`.system.cluster`)
- `meet(host, port)`: Form cluster partitions.
- `addSlots(...slots)`: Assign specific hash slots to the current node.
- `slots()`: View hash-slot assignments.
- `info()`: Replication state and node health.

### Persistence (`.system`)
- `save()`: Foreground snapshot.
- `rewriteAof()`: Background log optimization.

### Clients (`.system.clients`)
- `list()`: Connected clients.
- `kill(addr)`: Terminate specific connection.

---

## 🔄 Transactions (ACID)

Atomic multi-operation blocks.

```javascript
await db.beginTransaction();
try {
    await db.set("acc:1", 100);
    await db.set("acc:2", 200);
    await db.commit();
} catch (e) {
    await db.rollback();
}
```

---

## ⚠️ Error Handling

The client throws `ToriDBError` with useful metadata.

```javascript
try {
    // ... code
} catch (err) {
    console.log(err.code); // e.g., "SERVER_ERROR", "AUTH_FAILED"
    console.log(err.originalError); // Low-level socket/parser error
}
```

---
[Back to Document Index](../README.md)
