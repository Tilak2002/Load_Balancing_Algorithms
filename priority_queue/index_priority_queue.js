import cluster from "node:cluster";
import http from "http";

const numCPUs = 4;

if (cluster.isPrimary) {
  console.log(`🧠 Master PID: ${process.pid}`);

  const workers = {};
  const workerLoad = [];

  // Create workers with initial load 0
  for (let i = 0; i < numCPUs; i++) {
    const worker = cluster.fork();
    workers[worker.process.pid] = worker;
    workerLoad.push({ pid: worker.process.pid, load: 0 });

    worker.on("message", (msg) => {
      if (msg.type === "completed") {
        const w = workerLoad.find(w => w.pid === worker.process.pid);
        w.load = Math.max(0, w.load - msg.processingTime);
        console.log(`✅ Worker ${w.pid} completed task (${msg.processingTime}ms). Load: ${w.load}`);
      }
    });
  }

  // Create server
  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    if (req.url === "/load") {
      const processingTime = Math.floor(1000 + Math.random() * 4000);

      // 🥇 Priority Queue logic (select least loaded worker)
      workerLoad.sort((a, b) => a.load - b.load);
      const selected = workerLoad[0];
      selected.load += processingTime;

      const worker = workers[selected.pid];
      worker.send({ type: "handle_request", processingTime });

      console.log(`📨 Assigned (${processingTime}ms) to Worker ${selected.pid}. New Load: ${selected.load}ms`);
      res.writeHead(200);
      res.end(`Assigned to Worker ${selected.pid} | Time: ${processingTime}ms`);
    } else {
      res.writeHead(404);
      res.end("Not Found");
    }
  });

  server.listen(3000, () => {
    console.log("🌐 Server running at http://localhost:3000");
  });

} else {
  console.log(`🛠️ Worker PID: ${process.pid}`);

  process.on("message", (msg) => {
    if (msg.type === "handle_request") {
      const { processingTime } = msg;
      console.log(`⚙️ Worker ${process.pid} started (${processingTime}ms)`);

      setTimeout(() => {
        process.send({ type: "completed", processingTime });
      }, processingTime);
    }
  });
}
