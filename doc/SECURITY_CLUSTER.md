# Security & Clustering

This document covers user access control and the distributed nature of **ToriDB**.

## 1. Security (ACL & Auth)
The database implements a Role-Based Access Control (RBAC) system using Access Control Lists (ACLs).

### Authentication
Connections require authentication via the `AUTH` command. Passwords are hashed using **bcrypt** before storage.
```text
AUTH username password
```

### Access Control Lists (ACL)
Manage users and their permissions dynamically.
- `ACL SETUSER alice secret "get set *"`: Allow Alice 'get' and 'set' on all keys.
- `ACL LIST`: View all users.
- `ACL DELUSER alice`: Remove access.

## 2. Replication
Achieve High Availability (HA) through asynchronous Master-Replica replication.

### Setup
A node can become a replica of another using the `REPLICAOF` command or URI.
```bash
# Via command
REPLICAOF 127.0.0.1 8569
```
- **Partial Resync (PSYNC)**: Replicas maintain a persistent connection to receive real-time write propagation.
- **Full Resync**: On first connection, the master sends a complete snapshot followed by the redo log.

## 3. Clustering & Sharding
The system uses a **Slot-based Sharding** mechanism (16,384 slots) similar to Redis Cluster.

### Key Mapping
- Every key is hashed into a slot.
- `CLUSTER SLOTS`: See which nodes own which slot ranges.

### Redirection (MOVED)
If a client sends a command to the wrong node, the node responds with a `MOVED <slot> <ip:port>` error, guiding the client to the correct shard.

---
[Back to Home](./README.md)
