# ToriDB Protocol Specification (RESP+)

ToriDB uses a modified version of the **Redis Serialization Protocol (RESP)** for client-server communication. This protocol is binary-safe, easy to implement, and extremely fast.

## 1. Data Types
Both request and response use the following format indicators:

| Prefix | Type | Description | Example |
|--------|------|-------------|---------|
| `+` | Simple String | Success messages, short text | `+OK\r\n` |
| `-` | Error | Error code and message | `-ERR Syntax error\r\n` |
| `:` | Integer | Numeric responses (counts, etc) | `:42\r\n` |
| `$` | Bulk String | Binary-safe strings or JSON | `$5\r\nhello\r\n` |
| `*` | Array | Lists of elements or rows | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

---

## 2. Command Format
Clients send commands as a **RESP Array** of Bulk Strings.

**Example: `SET mykey myval`**
```text
*3\r\n
$3\r\n
SET\r\n
$5\r\n
mykey\r\n
$5\r\n
myval\r\n
```

---

## 3. High-Level Protocols

### 3.1 Handshake & Auth
1. **Connect**: TCP connection is established (Port 8569).
2. **Auth (Optional)**: `AUTH <user> <pass>`. Returns `+OK` or `-ERR`.
3. **Use (Optional)**: `USE <dbname>`. Selects the active dataset context.

### 3.2 Unified Connection URI
ToriDB clients should support the following URI format:
`db://[user]:[password]+[host]:[port]/[database][?options]`

- **Protocol**: `db://`
- **Separator**: Uses `+` between credentials and host to distinguish from standard HTTP.
- **Example**: `db://admin:secret+127.0.0.1:8569/production?workers=10`

---

## 4. Specific Engine Responses

### 4.1 SQL Result Sets
SQL `SELECT` commands return a **RESP Array** containing rows. Each row is itself an **Array** of values.

```text
*2\r\n      // 2 rows
*2\r\n      // Row 1
$5\r\nAlice\r\n
:30\r\n
*2\r\n      // Row 2
$3\r\nBob\r\n
:25\r\n
```

### 4.2 Vector Search
Vector searches return an **Array** of objects (typically stringified JSON) or rows, ordered by **Cosine Similarity** (descending).

---

## 5. Persistence Codes
- `+Snapshot saved`: Response to `SAVE`.
- `+AOF Rewrite started`: Response to `REWRITEAOF`.

---
[Back to Home](../README.md)
