# Security, Cluster & High Availability (⛩️)

This guide covers advanced administrative topics including Access Control Lists (ACLs), Cluster formation, and Data Replication.

## 1. Security & RBAC

ToriDB implements a robust **Role-Based Access Control (RBAC)** system modeled after Redis ACLs but enhanced for SQL multi-tenancy.

### 1.1 Managing Users
Users are created and modified using the `ACL` command family.

| Command | Description |
|---------|-------------|
| `ACL LIST` | Lists all active users and their rules. |
| `ACL SETUSER <user> <pass> <rules...>` | Creates or updates a user. |
| `ACL GETUSER <user>` | Shows details for a specific user. |
| `ACL DELUSER <user>` | Removes a user. |

### 1.2 Rule Syntax
Rules are defined as a list of strings prefixed with `+` (allow) or `-` (deny).

- `+@all`: Grants access to every command.
- `+get`: explicitly allows the `GET` command.
- `-delete`: explicitly denies the SQL `DELETE` command.
- `+data`: allows access to the `data` database only.

**Example: Creating a read-only operator**
```text
ACL SETUSER readonly pass123 +get +select +smembers -set -insert -delete
```

---

## 2. Multi-Node Clustering

ToriDB achieves horizontal scale through a share-nothing architecture using **Virtual Slots**.

### 2.1 The Slot Model
The keyspace is divided into **16,384 virtual slots**.
- **Hashing**: `slot = CRC16(key) % 16384`.
- **Ownership**: Every node in the cluster is assigned a range of slots.

### 2.2 Cluster Commands
- `CLUSTER MEET <host> <port>`: Explicitly joins two nodes into a cluster.
- `CLUSTER SLOTS`: Returns the mapping of slots to node IPs.
- `CLUSTER ADDSLOTS <slot...>`: Assigns specific slots to the current node.

### 2.3 Client Redirection
When a node receives a command for a key it doesn't own, it responds with a **MOVED** error:
`MOVED 3942 192.168.1.50:8569`
The ToriDB SDK handles these redirections automatically.

---

## 3. Replication & High Availability

ToriDB supports **Primary-Replica** replication for data redundancy and read-scaling.

### 3.1 Setup
A node can become a replica of another using the `REPLICAOF` command:
```text
REPLICAOF 192.168.1.10 8569
```

### 3.2 Synchronization Flow
1. **Handshake**: Replica connects and sends `PING`.
2. **PSYNC**: Subscriber requests the replication stream.
3. **Full Sync (Snapshot)**: If the replica is behind, the Master sends a JSON snapshot of the current state.
4. **Propagation Mode**: The Master forwards every write operation (AOF stream) to all connected replicas in real-time.

---
[Back to Home](../README.md)
