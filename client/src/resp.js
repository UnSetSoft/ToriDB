/**
 * High-Performance RESP (REdis Serialization Protocol) Parser
 * Handles Simple Strings, Errors, Integers, Bulk Strings, and Nested Arrays.
 */
class RespParser {
  /**
   * Initializes or resets the parser state.
   */
  constructor() {
    this.reset();
  }

  /**
   * Clears the current data buffer.
   */
  reset() {
    this.buffer = Buffer.alloc(0);
  }

  /**
   * Feeds raw binary data into the parser's buffer.
   * @param {Buffer} data - The raw data received from the socket.
   */
  feed(data) {
    this.buffer = Buffer.concat([this.buffer, data]);
  }

  /**
   * Tries to parse the next complete RESP value from the buffer.
   * @returns {any | undefined} The parsed value or undefined if incomplete.
   */
  parseNext() {
    if (this.buffer.length === 0) return undefined;

    const result = this._decode(0);
    if (result) {
      this.buffer = this.buffer.slice(result.bytesRead);
      return result.value;
    }
    return undefined;
  }

  /**
   * Decodes a single RESP value from the buffer starting at a given offset.
   * Internal recursive method for handling nested arrays and bulk strings.
   * @param {number} offset - The buffer offset to start parsing from.
   * @returns {Object|null} An object with {value, bytesRead} or null if incomplete.
   * @private
   */
  _decode(offset) {
    if (offset >= this.buffer.length) return null;

    const prefix = this.buffer[offset];
    const lineEnd = this.buffer.indexOf('\r\n', offset);
    if (lineEnd === -1) return null;

    const content = this.buffer.slice(offset + 1, lineEnd).toString();
    const bytesBeforeData = lineEnd - offset + 2;

    switch (prefix) {
      case 43: // '+' Simple String
        return { value: content, bytesRead: bytesBeforeData };

      case 45: // '-' Error
        return { value: new Error(content), bytesRead: bytesBeforeData };

      case 58: // ':' Integer
        return { value: parseInt(content, 10), bytesRead: bytesBeforeData };

      case 36: { // '$' Bulk String
        const length = parseInt(content, 10);
        if (length === -1) return { value: null, bytesRead: bytesBeforeData };
        
        const dataEnd = lineEnd + 2 + length;
        if (this.buffer.length < dataEnd + 2) return null;
        
        const value = this.buffer.slice(lineEnd + 2, dataEnd).toString();
        // Verify CRLF
        if (this.buffer[dataEnd] !== 13 || this.buffer[dataEnd + 1] !== 10) {
           throw new Error("Invalid Bulk String terminator");
        }
        return { value, bytesRead: bytesBeforeData + length + 2 };
      }

      case 42: { // '*' Array
        const count = parseInt(content, 10);
        if (count === -1) return { value: null, bytesRead: bytesBeforeData };
        if (count === 0) return { value: [], bytesRead: bytesBeforeData };

        let totalRead = bytesBeforeData;
        const items = [];
        for (let i = 0; i < count; i++) {
          const item = this._decode(offset + totalRead);
          if (!item) return null;
          items.push(item.value);
          totalRead += item.bytesRead;
        }
        return { value: items, bytesRead: totalRead };
      }

      default:
        // Attempt to handle unexpected data as a simple string line
        return { value: content, bytesRead: bytesBeforeData };
    }
  }

  /**
   * Serializes a command into a RESP Array.
   * @param {string[]} args 
   */
  static encode(args) {
    let res = `*${args.length}\r\n`;
    for (const arg of args) {
      const s = String(arg);
      res += `$${Buffer.byteLength(s)}\r\n${s}\r\n`;
    }
    return Buffer.from(res);
  }
}

export { RespParser };
