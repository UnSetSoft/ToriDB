const net = require('net');
const fs = require('fs');
const path = require('path');

const client = new net.Socket();

function sendCommand(cmd) {
    return new Promise((resolve) => {
        console.log(`> ${cmd}`);
        client.write(cmd + '\r\n');

        const handler = (data) => {
            const response = data.toString().trim();
            console.log(`< ${response}`);
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

        // 1. Set some Data
        await sendCommand('SET snapshot_key snapshot_value');
        await sendCommand('CREATE TABLE snap_users id:int:pk name:string');
        await sendCommand('INSERT snap_users 1 "SnapUser"');

        // 2. Trigger Save
        const response = await sendCommand('SAVE');

        if (response.includes('OK')) {
            console.log('Save command accepted.');

            // 3. Verify File Exists
            setTimeout(() => {
                const dumpPath = path.join(__dirname, '../data/data_dump.json');
                if (fs.existsSync(dumpPath)) {
                    console.log(`✅ SUCCESS: Dump file exists at ${dumpPath}`);
                    const content = fs.readFileSync(dumpPath, 'utf8');
                    const json = JSON.parse(content);

                    // Verify Flex Data
                    let hasKey = false;
                    for (const [k, v] of Object.entries(json.flexible_data)) {
                        if (k === 'snapshot_key' && v === 'snapshot_value') hasKey = true;
                    }

                    // Verify Table Data
                    let hasTable = false;
                    for (const [k, v] of Object.entries(json.structured_data)) {
                        if (k === 'snap_users') hasTable = true;
                    }

                    if (hasKey && hasTable) {
                        console.log('✅ SUCCESS: Data verified in JSON.');
                    } else {
                        console.error('❌ FAILURE: Data missing in JSON.');
                        console.log(content.substring(0, 200) + '...');
                    }

                } else {
                    console.error(`❌ FAILURE: Dump file NOT found at ${dumpPath}`);
                }
                client.end();
            }, 1000);
        } else {
            console.error('❌ FAILURE: SAVE command returned error.');
            client.end();
        }
    });
}

runTest();
