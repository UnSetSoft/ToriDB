const net = require('net');

const client = new net.Socket();
const PORT = 8569;

function sendCommand(command) {
    return new Promise((resolve, reject) => {
        client.write(command + '\n');
        client.once('data', (data) => {
            const resp = data.toString().trim();
            console.log(`REQ: ${command} -> RESP: ${resp}`);
            resolve(resp);
        });
        client.once('error', reject);
    });
}

async function runTests() {
    try {
        await new Promise((resolve) => client.connect(PORT, 'localhost', resolve));
        console.log('Connected to ToriDB');

        await sendCommand('AUTH default secret');

        // Test 1: Create table with JSON column
        console.log('\n--- Test 1: CREATE TABLE with JSON column ---');
        let createResult = await sendCommand('CREATE TABLE users_json id:int:pk data:json');
        // Note: Using unique table name to avoid conflicts

        // Test 2: Insert JSON data
        console.log('\n--- Test 2: INSERT JSON data ---');
        await sendCommand('INSERT users_json 1 \'{"name":"Alice","age":30,"city":"NYC"}\'');
        await sendCommand('INSERT users_json 2 \'{"name":"Bob","age":25,"city":"LA"}\'');
        await sendCommand('INSERT users_json 3 \'{"name":"Charlie","age":35,"city":"Chicago"}\'');

        // Test 3: Select all
        console.log('\n--- Test 3: SELECT * FROM users_json ---');
        let allRows = await sendCommand('SELECT * FROM users_json');
        console.log('All rows:', allRows);
        if (!allRows.includes('Alice') || !allRows.includes('Bob')) {
            throw new Error('JSON data not stored correctly');
        }

        console.log('\n=== ALL JSON COLUMN TESTS PASSED ===');
    } catch (err) {
        console.error('Test Failed:', err);
        process.exit(1);
    } finally {
        client.end();
    }
}

runTests();
