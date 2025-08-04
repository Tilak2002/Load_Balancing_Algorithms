import cluster from 'node:cluster';
import http from 'node:http';
import os from 'node:os';

const numCPUs = os.cpus().length;

if (cluster.isPrimary) {
  console.log(`🧠 Master PID: ${process.pid}`);
  const workers = {};
  const workerLoad = [];

  // Create workers with initial load 0
  for (let i = 0; i < numCPUs; i++) {
    const worker = cluster.fork();
    workers[worker.process.pid] = worker;
    workerLoad.push({ pid: worker.process.pid, load: 0 });

    worker.on('message', (msg) => {
      if (msg.type === 'completed') {
        const workerInfo = workerLoad.find(w => w.pid === msg.pid);
        if (workerInfo) {
          workerInfo.load = Math.max(0, workerInfo.load - msg.processingTime);
          console.log(`✅ Worker ${msg.pid} completed task (${msg.processingTime}ms). Load: ${workerInfo.load}ms`);
        }
      }
    });
  }

  // Create server
  const server = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');

    if (req.url === '/load') {
      const processingTime = Math.floor(1000 + Math.random() * 4000);

      // 🥇 Priority Queue logic (select least loaded worker)
      workerLoad.sort((a, b) => a.load - b.load);
      const selected = workerLoad[0];
      selected.load += processingTime;

      const worker = workers[selected.pid];
      worker.send({ type: 'handle_request', pid: selected.pid, processingTime });

      console.log(`📨 Assigned (${processingTime}ms) to Worker ${selected.pid}. New Load: ${selected.load}ms`);
      res.writeHead(200);
      res.end(`Assigned to Worker ${selected.pid} | Time: ${processingTime}ms`);
    } else {
      res.writeHead(404);
      res.end('Not Found');
    }
  });

  server.listen(3000, () => {
    console.log('🌐 Server running at http://localhost:3000');
  });

} else {
  // Worker process
  process.on('message', (msg) => {
    if (msg.type === 'handle_request') {
      const { pid, processingTime } = msg;
      console.log(`⚙️ Worker ${pid} started (${processingTime}ms)`);

      setTimeout(() => {
        process.send({ type: 'completed', pid, processingTime });
      }, processingTime);
    }
  });
}
