const net = require('net');

const client = new net.Socket();
const PORT = 8569; // Default ToriDB port

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

        // Auth (default user with password from DB_PASSWORD env or 'secret')
        await sendCommand('AUTH default secret');

        // Setup Table (Syntax: CREATE TABLE name col:type[:pk] col:type ...)
        let createResult = await sendCommand('CREATE TABLE test_types id:int:pk name:string age:int score:float active:bool');
        if (!createResult.includes('OK') && !createResult.includes('exists')) {
            throw new Error('Failed to create table');
        }

        // Insert with various types
        console.log('Inserting data...');
        await sendCommand('INSERT test_types 1 "Alice" 30 95.5 true');
        await sendCommand('INSERT test_types 2 "Bob" 25 88.0 true');
        await sendCommand('INSERT test_types 3 "Charlie" 35 72.5 false');
        await sendCommand('INSERT test_types 10 "David" 28 90.0 true');  // id=10 to test numeric sorting vs string

        console.log('Verifying Insert and Select...');
        let rows = await sendCommand('SELECT * FROM test_types');
        console.log('All Rows:', rows);
        if (!rows.includes('Alice') || !rows.includes('30') || !rows.includes('95.5')) {
            throw new Error('Insert basic data failed');
        }

        // Test ORDER BY with numeric column (id - should sort 1,2,3,10 NOT 1,10,2,3)
        console.log('Testing Numerical Sort (ORDER BY id)...');
        let sorted = await sendCommand('SELECT * FROM test_types ORDER BY id ASC');
        console.log('Sorted by ID:', sorted);
        // Check that 10 appears AFTER 3 in the response (numeric order, not lexicographic)
        const idx3 = sorted.indexOf('"3"') || sorted.indexOf('3');
        const idx10 = sorted.indexOf('"10"') || sorted.indexOf('10');
        if (idx10 < idx3) {
            throw new Error('Numerical sorting failed: 10 should come after 3');
        }
        console.log('Numerical sorting: 3 appears before 10 - PASS');

        // Test Integer Filter (age > 27)
        console.log('Testing Integer Filter (age > 27)...');
        let older = await sendCommand('SELECT * FROM test_types WHERE age > 27');
        console.log('Age > 27:', older);
        if (!older.includes('Alice') || !older.includes('Charlie') || !older.includes('David')) {
            throw new Error('Integer filter (age > 27) missed expected rows');
        }
        if (older.includes('Bob')) { // Bob is 25
            throw new Error('Integer filter (age > 27) should not include Bob');
        }

        // Test Float Filter (score < 90.0)
        console.log('Testing Float Filter (score < 90.0)...');
        let lowScore = await sendCommand('SELECT * FROM test_types WHERE score < 90.0');
        console.log('Score < 90:', lowScore);
        if (!lowScore.includes('Bob') || !lowScore.includes('Charlie')) {
            throw new Error('Float filter (score < 90) failed');
        }
        if (lowScore.includes('Alice') || lowScore.includes('David')) {
            throw new Error('Float filter (score < 90) should not include Alice or David');
        }

        // Test Boolean Filter
        console.log('Testing Boolean Filter (active = true)...');
        let activeUsers = await sendCommand('SELECT * FROM test_types WHERE active = true');
        console.log('Active users:', activeUsers);
        if (!activeUsers.includes('Alice') || activeUsers.includes('Charlie')) {
            throw new Error('Boolean filter failed');
        }

        // Test Update type preservation
        console.log('Testing Update type preservation...');
        await sendCommand('UPDATE test_types SET score = 99.9 WHERE id = 2');
        let updated = await sendCommand('SELECT * FROM test_types WHERE id = 2');
        console.log('Updated row:', updated);
        if (!updated.includes('99.9')) {
            throw new Error('Update failed or type not preserved');
        }

        console.log('\n=== ALL TESTS PASSED ===');
    } catch (err) {
        console.error('Test Failed:', err);
        process.exit(1);
    } finally {
        client.end();
    }
}

runTests();
