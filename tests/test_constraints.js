const net = require('net');

const client = new net.Socket();
let buffer = '';

function sendCommand(cmd) {
  return new Promise((resolve) => {
    console.log(`> ${cmd}`);
    client.write(cmd + '\r\n');

    const handler = (data) => {
      const response = data.toString();
      console.log(`< ${response.trim()}`);
      client.removeListener('data', handler);
      resolve(response);
    };
    client.on('data', handler);
  });
}

async function runTest() {
  client.connect(8569, '127.0.0.1', async () => {
    console.log('Connected.');

    await sendCommand('AUTH default secret');

    // 1. Setup Table with Index
    // Creating a table with ID (PK) and Email (to be indexed implictly or explicitly later)
    // For now, let's create a table and an index manually if needed or verify current behavior.
    await sendCommand('CREATE TABLE users id:int:pk email:string age:int');
    await sendCommand('CREATE INDEX idx_users_email ON users(email)');

    // 2. Insert Data
    // Row 0
    await sendCommand('INSERT users 1 "alice@example.com" 30');
    // Row 1
    await sendCommand('INSERT users 2 "bob@example.com" 25');
    // Row 2
    await sendCommand('INSERT users 3 "charlie@example.com" 40');

    console.log('\n--- State: 3 Users inserted ---');
    // Verify Scan Select
    await sendCommand('SELECT * FROM users');

    // Verify Index Select (bob)
    console.log('\n--- Test Index Lookups (Before Delete) ---');
    await sendCommand('SELECT * FROM users WHERE email = "bob@example.com"');

    // 3. Delete Row 0 (Alice)
    console.log('\n--- Deleting Alice (Row 0) ---');
    await sendCommand('DELETE FROM users WHERE id = 1');

    // 4. Verify Integrity
    console.log('\n--- Test 1: Scan Select (Should show Bob and Charlie) ---');
    await sendCommand('SELECT * FROM users');

    console.log('\n--- Test 2: Index Lookup for Bob (Row 1 -> shifts to 0?) ---');
    // If indices use stored index '1', but Bob is now at '0', this might fail or crash
    await sendCommand('SELECT * FROM users WHERE email = "bob@example.com"');

    console.log('\n--- Test 3: Index Lookup for Charlie (Row 2 -> shifts to 1?) ---');
    await sendCommand('SELECT * FROM users WHERE email = "charlie@example.com"');

    client.end();
  });
}

runTest();
