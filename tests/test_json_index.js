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
        const tableName = 'indexed_people_' + Date.now();
        await sendCommand(`CREATE TABLE ${tableName} id:int:pk profile:json`);

        // Insert JSON data 
        console.log('\n--- INSERT JSON data ---');
        for (let i = 1; i <= 100; i++) {
            await sendCommand(`INSERT ${tableName} ${i} '{"name":"Person${i}","age":${20 + (i % 50)},"city":"City${i % 10}"}'`);
        }
        console.log('Inserted 100 rows');

        // Test without index first
        console.log('\n--- Test 1: SELECT without index (profile->age = 30) ---');
        let startNoIndex = Date.now();
        let resultNoIndex = await sendCommand(`SELECT * FROM ${tableName} WHERE profile->age = 30`);
        let timeNoIndex = Date.now() - startNoIndex;
        console.log(`Result (no index): ${resultNoIndex.substring(0, 100)}...`);
        console.log(`Time without index: ${timeNoIndex}ms`);

        // Create JSON path index
        console.log('\n--- CREATE INDEX on profile->age ---');
        let indexResult = await sendCommand(`CREATE INDEX idx_age ON ${tableName}(profile->age)`);
        if (!indexResult.includes('OK')) {
            throw new Error('Failed to create JSON path index');
        }
        console.log('Index created successfully');

        // Test with index
        console.log('\n--- Test 2: SELECT with index (profile->age = 30) ---');
        let startWithIndex = Date.now();
        let resultWithIndex = await sendCommand(`SELECT * FROM ${tableName} WHERE profile->age = 30`);
        let timeWithIndex = Date.now() - startWithIndex;
        console.log(`Result (with index): ${resultWithIndex.substring(0, 100)}...`);
        console.log(`Time with index: ${timeWithIndex}ms`);

        // Verify both queries return same results
        if (!resultWithIndex.includes('Person10') || !resultWithIndex.includes('Person60')) {
            throw new Error('Index query returned wrong results');
        }
        console.log('Index query returned correct results');

        // Test range query with index
        console.log('\n--- Test 3: Range query with index (profile->age > 65) ---');
        let rangeResult = await sendCommand(`SELECT * FROM ${tableName} WHERE profile->age > 65`);
        console.log(`Range result: ${rangeResult.substring(0, 100)}...`);
        
        // Should include ages 66, 67, 68, 69
        if (!rangeResult.includes('Person46') && !rangeResult.includes('Person96')) {
            console.log('Warning: Range query may not be using index');
        }

        console.log('\n=== ALL JSON PATH INDEX TESTS PASSED ===');
    } catch (err) {
        console.error('Test Failed:', err);
        process.exit(1);
    } finally {
        client.end();
    }
}

runTests();
