const net = require('net');
const { RespParser } = require('./resp');
const { Compiler } = require('./compiler');

/**
 * Main client class for interacting with ToriDB.
 * Supports Key-Value, NoSQL structures (List, Set, Hash, JSON), and Relational modeling.
 */
class ToriDB {
    /**
     * Represents a database schema definition.
     */
    static Blueprint = class {
        /**
         * @param {Object} definition - The schema definition object.
         */
        constructor(definition) {
            this.definition = definition;
        }
    };

    /**
     * Creates a new ToriDB client instance.
     * @param {string} [uri="db://127.0.0.1:8569"] - Connection URI (e.g., db://user:pass@host:port/db)
     */
    constructor(uri = "db://127.0.0.1:8569") {
        this.host = "127.0.0.1";
        this.port = 8569;
        this.user = "default";
        this.password = null;
        this._parseUri(uri);

        this.parser = new RespParser();
        this.socket = new net.Socket();
        this.isConnected = false;
        this.pendingRequests = [];

        this._setupSocket();

        // Managers
        this.system = new SystemManager(this);
    }

    /**
     * Parses the connection URI.
     * @param {string} uri - The connection URI to parse.
     * @private
     */
    _parseUri(uri) {
        const regex = /^db:\/\/(?:(?:([^:]+):([^@+]+)[@+])?)([^:/\?]+)(?::(\d+))?(?:(?:\/([^?]+))?)?/;
        const match = uri.match(regex);
        if (match) {
            this.user = match[1] || "default";
            this.password = match[2] || null;
            this.host = match[3];
            this.port = parseInt(match[4], 10) || 8569;
        }
    }

    /**
     * Sets up socket events for data handling, errors, and closure.
     * @private
     */
    _setupSocket() {
        this.socket.on('data', (data) => {
            this.parser.feed(data);
            let response;
            while ((response = this.parser.parseNext()) !== undefined) {
                const req = this.pendingRequests.shift();
                if (req) {
                    if (response instanceof Error) req.reject(response);
                    else req.resolve(response);
                }
            }
        });

        this.socket.on('error', (err) => {
            this.isConnected = false;
            this.pendingRequests.forEach(req => req.reject(err));
            this.pendingRequests = [];
        });

        this.socket.on('close', () => {
            this.isConnected = false;
        });
    }

    /**
     * Establishes a connection to the ToriDB server.
     * @returns {Promise<void>}
     * @throws {Error} If connection or authentication fails.
     */
    async connect() {
        if (this.isConnected) return;
        return new Promise((resolve, reject) => {
            this.socket.connect(this.port, this.host, async () => {
                this.isConnected = true;
                if (this.password) {
                    try {
                        await this.execute("AUTH", this.user, this.password);
                    } catch (e) {
                        this.disconnect();
                        return reject(new Error(`Authentication failed: ${e.message}`));
                    }
                }
                resolve();
            });
            this.socket.once('error', reject);
        });
    }

    /**
     * Disconnects from the ToriDB server.
     */
    disconnect() {
        this.socket.destroy();
        this.isConnected = false;
    }

    /**
     * Executes a command on the ToriDB server.
     * @param {...any} args - Command arguments.
     * @returns {Promise<any>} The server response.
     */
    async execute(...args) {
        if (!this.isConnected) await this.connect();
        return new Promise((resolve, reject) => {
            this.pendingRequests.push({ resolve, reject });
            this.socket.write(RespParser.encode(args));
        });
    }

    /**
     * Executes a raw query string.
     * @param {string} s - The query string.
     * @returns {Promise<any>}
     */
    async query(s) {
        return this.execute(...s.match(/(?:[^\s"]+|"[^"]*")+/g).map(p => p.replace(/"/g, '')));
    }

    // --- Key-Value & TTL ---
    /**
     * Retrieves the value of a key.
     * @param {string} key - The key to get.
     * @returns {Promise<any>}
     */
    async get(key) { return this.execute("GET", key); }

    /**
     * Sets the value of a key.
     * @param {string} key - The key to set.
     * @param {any} val - The value to store (objects are JSON-stringified).
     * @returns {Promise<string>} OK on success.
     */
    async set(key, val) {
        const s = typeof val === 'object' ? JSON.stringify(val) : String(val);
        return this.execute("SET", key, s);
    }
    /**
     * Sets the value of a key with an expiration time.
     * @param {string} key - The key to set.
     * @param {any} val - The value to store.
     * @param {number} ttl - Time to live in seconds.
     * @returns {Promise<string>} OK on success.
     */
    async setEx(key, val, ttl) {
        const s = typeof val === 'object' ? JSON.stringify(val) : String(val);
        return this.execute("SETEX", key, s, String(ttl));
    }
    /**
     * Gets the remaining time to live of a key.
     * @param {string} key - The key.
     * @returns {Promise<number>} TTL in seconds, or -1 if no TTL, or -2 if key doesn't exist.
     */
    async ttl(key) { return this.execute("TTL", key); }

    /**
     * Increments the integer value of a key by one.
     * @param {string} key - The key to increment.
     * @returns {Promise<number>} The value after increment.
     */
    async incr(key) { return this.execute("INCR", key); }

    /**
     * Decrements the integer value of a key by one.
     * @param {string} key - The key to decrement.
     * @returns {Promise<number>} The value after decrement.
     */
    async decr(key) { return this.execute("DECR", key); }

    // --- NoSQL Structures ---
    /**
     * Accesses list operations for a given key.
     * @param {string} key - The list key.
     * @returns {Object} An object with list operations (push, rpush, pop, rpop, range).
     */
    list(key) {
        return {
            push: (...vals) => this.execute("LPUSH", key, ...vals.map(String)),
            rpush: (...vals) => this.execute("RPUSH", key, ...vals.map(String)),
            pop: (count = 1) => this.execute("LPOP", key, String(count)),
            rpop: (count = 1) => this.execute("RPOP", key, String(count)),
            range: (start, stop) => this.execute("LRANGE", key, String(start), String(stop))
        };
    }

    /**
     * Accesses set operations for a given key.
     * @param {string} key - The set key.
     * @returns {Object} An object with set operations (add, members).
     */
    setOf(key) {
        return {
            add: (...members) => this.execute("SADD", key, ...members.map(String)),
            members: () => this.execute("SMEMBERS", key)
        };
    }

    /**
     * Accesses hash operations for a given key.
     * @param {string} key - The hash key.
     * @returns {Object} An object with hash operations (set, get, all).
     */
    hash(key) {
        return {
            set: (field, val) => this.execute("HSET", key, field, String(val)),
            get: (field) => this.execute("HGET", key, field),
            all: () => this.execute("HGETALL", key)
        };
    }

    /**
     * Accesses sorted set operations for a given key.
     * @param {string} key - The sorted set key.
     * @returns {Object} An object with sorted set operations (add, range, score).
     */
    sortedSet(key) {
        return {
            add: (score, member) => this.execute("ZADD", key, String(score), member),
            range: (start, stop) => this.execute("ZRANGE", key, String(start), String(stop)),
            score: (member) => this.execute("ZSCORE", key, member)
        };
    }

    /**
     * Accesses JSON operations for a given key.
     * @param {string} key - The JSON key.
     * @returns {Object} An object with JSON operations (get, set).
     */
    json(key) {
        return {
            get: (path = "$") => this.execute("JSON.GET", key, path),
            set: (path, val) => {
                const s = typeof val === 'object' ? JSON.stringify(val) : JSON.stringify(val);
                return this.execute("JSON.SET", key, path, s);
            }
        };
    }

    // --- Relational (Models) ---
    /**
     * Defines or accesses a model for relational-like operations.
     * @param {string} name - The model name (table name).
     * @param {ToriDB.Blueprint} blueprint - The schema definition.
     * @returns {Object} An object with model operations (create, find, findById, update, delete, etc.).
     */
    model(name, blueprint) {
        const createCmd = Compiler.compileBlueprint(name, blueprint.definition);
        this.execute(...createCmd.match(/(?:[^\s"]+|"[^"]*")+/g).map(p => p.replace(/"/g, ''))).catch(() => { });

        return {
            create: (data) => this.execute("INSERT", name, ...Object.values(data).map(v => typeof v === 'object' ? JSON.stringify(v) : String(v))),
            find: (filter) => new QueryBuilder(this, name, filter),
            findById: (id) => new QueryBuilder(this, name, { id }).execute().then(r => r[0] || null),
            update: (filter, data) => {
                const where = Compiler.compileFilter(filter);
                const [col, val] = Object.entries(data)[0];
                const sVal = typeof val === 'object' ? JSON.stringify(val) : String(val);
                return this.execute("UPDATE", name, "SET", col, "=", sVal, "WHERE", where);
            },
            delete: (filter) => this.execute("DELETE", "FROM", name, "WHERE", Compiler.compileFilter(filter)),
            createIndex: (idxName, col) => this.execute("CREATE", "INDEX", idxName, "ON", name, `(${col})`),
            addColumn: (col, type) => this.execute("ALTER", "TABLE", name, "ADD", `${col}:${type}`),
            dropColumn: (col) => this.execute("ALTER", "TABLE", name, "DROP", col)
        };
    }

    /**
     * Accesses a table for query operations.
     * @param {string} name - The table name.
     * @returns {Object} An object with a find method.
     */
    table(name) {
        return { find: (filter) => new QueryBuilder(this, name, filter) };
    }
}

/**
 * Manager for system-level operations (ACL, Cluster, Replication, etc.).
 */
class SystemManager {
    /**
     * @param {ToriDB} client - The ToriDB client instance.
     */
    constructor(client) { this.client = client; }

    /**
     * Retrieves server information and statistics.
     * @returns {Promise<string>}
     */
    async info() { return this.client.execute("INFO"); }

    /**
     * Synchronously saves the dataset to disk.
     * @returns {Promise<string>}
     */
    async save() { return this.client.execute("SAVE"); }

    /**
     * Triggers an AOF rewrite in the background.
     * @returns {Promise<string>}
     */
    async rewriteAof() { return this.client.execute("REWRITEAOF"); }

    /**
     * Accesses ACL (Access Control List) operations.
     * @returns {Object} An object with ACL operations (createUser, getUser, listUsers, deleteUser).
     */
    get acl() {
        return {
            createUser: (u, p, rules) => this.client.execute("ACL", "SETUSER", u, p, ...rules),
            getUser: (u) => this.client.execute("ACL", "GETUSER", u),
            listUsers: () => this.client.execute("ACL", "LIST"),
            deleteUser: (u) => this.client.execute("ACL", "DELUSER", u)
        };
    }

    /**
     * Accesses cluster management operations.
     * @returns {Object} An object with cluster operations (meet, slots, info).
     */
    get cluster() {
        return {
            meet: (h, p) => this.client.execute("CLUSTER", "MEET", h, String(p)),
            slots: () => this.client.execute("CLUSTER", "SLOTS"),
            info: () => this.client.execute("CLUSTER", "INFO")
        };
    }

    /**
     * Accesses replication management operations.
     * @returns {Object} An object with replication operations (slaveOf, stop).
     */
    get replication() {
        return {
            slaveOf: (h, p) => this.client.execute("REPLICAOF", h, String(p)),
            stop: () => this.client.execute("REPLICAOF", "NO", "ONE")
        };
    }

    /**
     * Accesses client management operations.
     * @returns {Object} An object with client operations (list, kill).
     */
    get clients() {
        return {
            list: () => this.client.execute("CLIENT", "LIST"),
            kill: (addr) => this.client.execute("CLIENT", "KILL", addr)
        };
    }
}

/**
 * Helper class for building and executing SQL-like queries.
 */
class QueryBuilder {
    /**
     * @param {ToriDB} client - The ToriDB client instance.
     * @param {string} target - The table or model name.
     * @param {Object} [filter={}] - Initial filter object.
     */
    constructor(client, target, filter = {}) {
        this.client = client;
        this.target = target;
        this.params = {
            filter: Compiler.compileFilter(filter),
            limit: null,
            offset: null,
            orderBy: null,
            groupBy: null,
            select: "*"
        };
    }

    /**
     * Specifies the fields to select.
     * @param {string|string[]|Object} fields - Fields to select.
     * @returns {QueryBuilder} This instance for chaining.
     */
    select(fields) {
        if (typeof fields === 'string') this.params.select = fields;
        else if (Array.isArray(fields)) this.params.select = fields.join(", ");
        else if (typeof fields === 'object') {
            this.params.select = Object.entries(fields).map(([k, v]) => `${v} AS ${k}`).join(", ");
        }
        return this;
    }

    /**
     * Limits the number of results.
     * @param {number} n - The limit count.
     * @returns {QueryBuilder} This instance for chaining.
     */
    limit(n) { this.params.limit = n; return this; }

    /**
     * Offsets the results.
     * @param {number} n - The offset count.
     * @returns {QueryBuilder} This instance for chaining.
     */
    offset(n) { this.params.offset = n; return this; }

    /**
     * Orders the results by a column.
     * @param {string} col - The column name.
     * @param {string} [dir="ASC"] - Sort direction (ASC or DESC).
     * @returns {QueryBuilder} This instance for chaining.
     */
    orderBy(col, dir = "ASC") { this.params.orderBy = `${col} ${dir.toUpperCase()}`; return this; }

    /**
     * Groups the results by columns.
     * @param {string|string[]} cols - The columns to group by.
     * @returns {QueryBuilder} This instance for chaining.
     */
    groupBy(cols) { this.params.groupBy = Array.isArray(cols) ? cols.join(", ") : cols; return this; }

    /**
     * Compiles and executes the query.
     * @returns {Promise<any[]>} The query results.
     */
    async execute() {
        const args = ["SELECT", this.params.select, "FROM", this.target];
        if (this.params.filter) { args.push("WHERE"); args.push(this.params.filter); }
        if (this.params.groupBy) { args.push("GROUP"); args.push("BY"); args.push(this.params.groupBy); }
        if (this.params.orderBy) { args.push("ORDER"); args.push("BY"); args.push(this.params.orderBy); }
        if (this.params.limit) { args.push("LIMIT"); args.push(String(this.params.limit)); }
        if (this.params.offset) { args.push("OFFSET"); args.push(String(this.params.offset)); }

        return this.client.execute(...args);
    }
}

module.exports = { ToriDB };
