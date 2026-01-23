/**
 * Command Compiler: Translates JS Objects and Types to ToriDB DSL
 */
class Compiler {
  /**
   * Translates a JS Object to a ToriDB Filter string.
   * { age: { $gt: 18 }, status: 'active' } -> "age > 18 AND status = 'active'"
   */
  static compileFilter(filter) {
    if (!filter || typeof filter !== 'object') return "";

    const parts = [];
    for (const [key, value] of Object.entries(filter)) {
      if (key === '$or' && Array.isArray(value)) {
        parts.push(`(${value.map(f => this.compileFilter(f)).join(" OR ")})`);
      } else if (key === '$and' && Array.isArray(value)) {
        parts.push(`(${value.map(f => this.compileFilter(f)).join(" AND ")})`);
      } else if (typeof value === 'object' && value !== null) {
        // Handle operators: { $gt: 10 }
        for (const [op, val] of Object.entries(value)) {
          const symbol = this._mapOperator(op);
          parts.push(`${key} ${symbol} ${this._escape(val)}`);
        }
      } else {
        // Basic equality
        parts.push(`${key} = ${this._escape(value)}`);
      }
    }
    return parts.join(" AND ");
  }

  /**
   * Translates a Blueprint definition to ToriDB Table Definition format.
   * { id: { type: 'INT', primary: true }, user_id: { type: 'INT', references: 'users.id' } }
   * -> "id:INT:pk user_id:INT:fk(users.id)"
   */
  static compileBlueprint(name, blueprint) {
    const cols = [];
    for (const [colName, config] of Object.entries(blueprint)) {
      let type = typeof config === 'string' ? config : config.type;
      let pk = (config.primary || config.is_pk) ? ":pk" : "";
      let fk = "";

      if (config.references) {
        const [refTable, refCol] = config.references.split('.');
        fk = `:fk(${refTable}.${refCol})`;
      }

      // Auto-map JS types if not specified as SQL types
      if (type === 'Number') type = 'INT';
      if (type === 'String') type = 'STRING';
      if (type === 'Object') type = 'JSON';
      if (type === 'Boolean') type = 'BOOL';

      cols.push(`${colName}:${type}${pk}${fk}`);
    }
    return `CREATE TABLE ${name} ${cols.join(" ")}`;
  }

  /**
   * Maps MongoDB-style operators to ToriDB SQL operators.
   * @param {string} op - The operator to map (e.g., "$gt").
   * @returns {string} The corresponding SQL operator or the operator itself if no map exists.
   * @private
   */
  static _mapOperator(op) {
    const maps = {
      '$gt': '>', '$gte': '>=', '$lt': '<', '$lte': '<=',
      '$ne': '!=', '$eq': '=', '$like': 'LIKE', '$in': 'IN'
    };
    return maps[op] || op;
  }

  /**
   * Escapes values for safe inclusion in ToriDB queries.
   * Handles strings, arrays, and objects (JSON).
   * @param {any} val - The value to escape.
   * @returns {string} The escaped and formatted value string.
   * @private
   */
  static _escape(val) {
    if (typeof val === 'string') {
      // If it's a list for IN operator, don't wrap the whole thing in quotes
      // but wrap individual elements if they are strings.
      // However, the server's IN expects (val1,val2)
      return `"${val.replace(/"/g, '\\"')}"`;
    }
    if (Array.isArray(val)) {
      return `(${val.map(v => this._escape(v)).join(",")})`;
    }
    if (typeof val === 'object' && val !== null) return `"${JSON.stringify(val).replace(/"/g, '\\"')}"`;
    return String(val);
  }
}

module.exports = { Compiler };
