const net = require('net');

async function testRestore() {
    const client = new net.Socket();
    
    const send = (cmd) => new Promise(resolve => {
        client.write(cmd + '\r\n');
        client.once('data', d => resolve(d.toString().trim()));
    });

    client.connect(8569, '127.0.0.1', async () => {
        console.log('Connected.');
        await send('AUTH default secret');
        
        // 1. Check Key
        const val = await send('GET snapshot_key');
        console.log(`GET snapshot_key: ${val}`);
        
        // 2. Check Table Row
        const row = await send('SELECT * FROM snap_users WHERE id = 1');
        console.log(`SELECT snap_users: ${row}`);
        
        if (val.includes('snapshot_value') && row.includes('SnapUser')) {
            console.log('✅ SUCCESS: Data restored from snapshot.');
        } else {
            console.error('❌ FAILURE: Data missing.');
        }
        
        client.end();
    });
}

testRestore();
