const { ToriDB } = require('../client/src/sdk');

async function runBenchmark() {
    const client = new ToriDB("db://default:secret+127.0.0.1:8569/bench");
    await client.connect();

    const iterations = 5000;
    console.log(`--- ToriDB Performance Benchmark (${iterations} iterations) ---`);

    // 1. KV Benchmark
    let start = Date.now();
    for (let i = 0; i < iterations; i++) {
        await client.set(`key:${i}`, `value-${i}`);
    }
    let end = Date.now();
    console.log(`KV SET: ${((iterations / (end - start)) * 1000).toFixed(2)} ops/sec`);

    start = Date.now();
    for (let i = 0; i < iterations; i++) {
        await client.get(`key:${i}`);
    }
    end = Date.now();
    console.log(`KV GET: ${((iterations / (end - start)) * 1000).toFixed(2)} ops/sec`);

    // 2. SQL Benchmark
    await client.execute("CREATE", "TABLE", "bench_sql", "id:int:pk", "val:string");
    start = Date.now();
    for (let i = 0; i < iterations; i++) {
        await client.table("bench_sql").create({ id: i, val: `text-${i}` });
    }
    end = Date.now();
    console.log(`SQL INSERT: ${((iterations / (end - start)) * 1000).toFixed(2)} ops/sec`);

    start = Date.now();
    for (let i = 0; i < 1000; i++) {
        await client.table("bench_sql").findById(i);
    }
    end = Date.now();
    console.log(`SQL SELECT (PK): ${((1000 / (end - start)) * 1000).toFixed(2)} ops/sec`);

    // 3. Vector Benchmark
    await client.execute("CREATE", "TABLE", "bench_vec", "id:int:pk", "emb:vector");
    for (let i = 0; i < 500; i++) {
        await client.execute("INSERT", "bench_vec", String(i), `[${Math.random()}, ${Math.random()}, ${Math.random()}]`);
    }
    
    start = Date.now();
    for (let i = 0; i < 100; i++) {
        await client.table("bench_vec").search("emb", [0.5, 0.5, 0.5], 5);
    }
    end = Date.now();
    console.log(`Vector SEARCH (500 items): ${((100 / (end - start)) * 1000).toFixed(2)} ops/sec`);

    client.disconnect();
}

runBenchmark().catch(console.error);
