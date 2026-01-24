const net = require('net');

const client = new net.Socket();
const send = (cmd) => new Promise(resolve => {
    client.write(cmd + '\r\n');
    client.once('data', d => resolve(d.toString().trim()));
});

async function run() {
    client.connect(8569, '127.0.0.1', async () => {
        console.log('Connected');
        await send('AUTH default secret');
        
        const arg = process.argv[2];
        if (arg === 'write') {
             console.log(await send('SET aof_check "persistent_value"'));
             console.log('Wrote key.');
        } else if (arg === 'read') {
             const res = await send('GET aof_check');
             console.log(`GET result: ${res}`);
             if (res.includes('persistent_value')) {
                 console.log('✅ AOF Replay Success');
             } else {
                 console.log('❌ AOF Replay Failed');
             }
        }
        client.end();
    });
}
run();
