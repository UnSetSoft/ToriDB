const net = require('net');

/**
 * Robust Multi-Model Database SDK for Node.js
 * Supports RESP protocol and db:// URI format.
 */
class DbClient {
    /**
     * @param {string} uri - Format: db://[user:pass+]host[:port][/dbName]
     */
    constructor(uri = "db://127.0.0.1:8569/data") {
        this.host = "127.0.0.1";
        this.port = 8569;
        this.user = null;
        this.password = null;
        this.dbName = "data";
        this.client = new net.Socket();
        this.isConnected = false;
        this.pendingResolvers = [];
        this.buffer = Buffer.alloc(0);

        this._parseUri(uri);
        this._setupListeners();
    }

    _parseUri(uri) {
        const regex = /^db:\/\/(?:(?:([^:]+):([^@+]+)[@+])?)([^:/\?]+)(?::(\d+))?(?:(?:\/([^?]+))?)?/;
        const match = uri.match(regex);
        if (match) {
            this.user = match[1] || null;
            this.password = match[2] || null;
            this.host = match[3];
            this.port = parseInt(match[4]) || 8569;
            this.dbName = match[5] || "data";
        } else {
            throw new Error("Invalid connection URI. Format: db://[user:pass+]host[:port][/dbName]");
        }
    }

    _setupListeners() {
        this.client.on('data', (data) => {
            this.buffer = Buffer.concat([this.buffer, data]);
            this._trySliceResponse();
        });

        this.client.on('close', () => {
            this.isConnected = false;
            // Reject any pending promises if connection closes unexpectedly
            while (this.pendingResolvers.length > 0) {
                const { reject } = this.pendingResolvers.shift();
                reject(new Error("Connection closed"));
            }
        });

        this.client.on('error', (err) => {
            console.error("SDK Socket Error:", err.message);
        });
    }

    _trySliceResponse() {
        // Simple RESP slicing based on CRLF
        // A production parser would be more stateful for Bulk Strings and Arrays
        while (this.buffer.length > 0) {
            const index = this.buffer.indexOf('\r\n');
            if (index === -1) break;

            const line = this.buffer.slice(0, index + 2).toString();
            const prefix = line[0];
            
            // For Simple Strings, Integers, and Errors, one line is enough
            if (prefix === '+' || prefix === '-' || prefix === ':') {
                this.buffer = this.buffer.slice(index + 2);
                this._resolveNext(this._parseSimple(line));
            } 
            // For Bulk Strings ($length\r\nvalue\r\n)
            else if (prefix === '$') {
                const length = parseInt(line.slice(1));
                if (length === -1) {
                    this.buffer = this.buffer.slice(index + 2);
                    this._resolveNext(null);
                } else {
                    const totalSize = index + 2 + length + 2;
                    if (this.buffer.length >= totalSize) {
                        const content = this.buffer.slice(index + 2, index + 2 + length).toString();
                        this.buffer = this.buffer.slice(totalSize);
                        this._resolveNext(content);
                    } else {
                        break; // Wait for more data
                    }
                }
            }
            // For Arrays (*length\r\n...) - Minimal support for results
            else if (prefix === '*') {
                // For now, we return the raw array data for complex CLI usage
                // In a full SDK, we would recursively parse elements.
                this.buffer = this.buffer.slice(index + 2);
                this._resolveNext(line.trim()); 
            } else {
                // Unknown/Raw
                this.buffer = this.buffer.slice(index + 2);
                this._resolveNext(line.trim());
            }
        }
    }

    _resolveNext(val) {
        if (this.pendingResolvers.length > 0) {
            const { resolve } = this.pendingResolvers.shift();
            resolve(val);
        }
    }

    _parseSimple(line) {
        const prefix = line[0];
        const content = line.slice(1).trim();
        if (prefix === '-') return `(error) ${content}`;
        if (prefix === ':') return parseInt(content);
        return content;
    }

    /**
     * Connects to the database and performs authentication.
     */
    async connect() {
        if (this.isConnected) return;

        return new Promise((resolve, reject) => {
            this.client.connect(this.port, this.host, async () => {
                this.isConnected = true;
                if (this.password) {
                    try {
                        const args = this.user ? ["AUTH", this.user, this.password] : ["AUTH", this.password];
                        const res = await this.execute(...args);
                        if (String(res).includes("error")) {
                            reject(new Error(`Auth failed: ${res}`));
                            return;
                        }
                    } catch (e) {
                        reject(e);
                        return;
                    }
                }
                resolve();
            });

            this.client.once('error', reject);
        });
    }

    /**
     * Executes a command.
     * @param {...string} args - Command and its arguments.
     */
    async execute(...args) {
        if (!this.isConnected) await this.connect();

        let cmd = `*${args.length}\r\n`;
        for (const arg of args) {
            const s = String(arg);
            cmd += `$${Buffer.byteLength(s)}\r\n${s}\r\n`;
        }

        return new Promise((resolve, reject) => {
            this.pendingResolvers.push({ resolve, reject });
            this.client.write(cmd);
        });
    }

    close() {
        this.client.destroy();
        this.isConnected = false;
    }
}

module.exports = { DbClient };
