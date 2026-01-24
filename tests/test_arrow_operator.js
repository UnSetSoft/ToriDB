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

        // Create table with JSON column (use unique name)
        console.log('\n--- Setup: CREATE TABLE ---');
        const tableName = 'people_' + Date.now();
        await sendCommand(`CREATE TABLE ${tableName} id:int:pk profile:json`);

        // Insert JSON data with different ages
        console.log('\n--- INSERT JSON data ---');
        await sendCommand(`INSERT ${tableName} 1 '{"name":"Alice","age":30,"city":"NYC"}'`);
        await sendCommand(`INSERT ${tableName} 2 '{"name":"Bob","age":25,"city":"LA"}'`);
        await sendCommand(`INSERT ${tableName} 3 '{"name":"Charlie","age":35,"city":"Chicago"}'`);
        await sendCommand(`INSERT ${tableName} 4 '{"name":"David","age":22,"city":"Boston"}'`);

        // Test 1: Arrow operator in WHERE clause
        console.log('\n--- Test 1: SELECT with profile->age > 27 ---');
        let result = await sendCommand(`SELECT * FROM ${tableName} WHERE profile->age > 27`);
        console.log('Result:', result);

        // Should include Alice (30), Charlie (35)
        // Should NOT include Bob (25), David (22)
        if (!result.includes('Alice') || !result.includes('Charlie')) {
            throw new Error('Arrow filter: Missing expected rows (Alice, Charlie)');
        }
        if (result.includes('Bob') || result.includes('David')) {
            throw new Error('Arrow filter: Included unexpected rows (Bob, David)');
        }
        console.log('Test 1 PASSED: Arrow filter works correctly');

        // Test 2: Arrow operator with string comparison
        console.log('\n--- Test 2: SELECT with profile->city = "LA" ---');
        let cityResult = await sendCommand(`SELECT * FROM ${tableName} WHERE profile->city = LA`);
        console.log('City Result:', cityResult);

        if (!cityResult.includes('Bob')) {
            throw new Error('Arrow string filter: Missing expected row (Bob)');
        }
        console.log('Test 2 PASSED: Arrow string filter works');

        console.log('\n=== ALL ARROW OPERATOR TESTS PASSED ===');
    } catch (err) {
        console.error('Test Failed:', err);
        process.exit(1);
    } finally {
        client.end();
    }
}

runTests();
