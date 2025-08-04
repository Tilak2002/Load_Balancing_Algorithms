// index_sjf.js
import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
  console.log(`🧠 Master process PID: ${process.pid}`);
  console.log(`🚀 Starting ${numCPUs} workers...`);

  const workers = {};
  const workerLoad = {}; // PID -> total estimated processing time

  for (let i = 0; i < numCPUs; i++) {
    const worker = cluster.fork();
    workers[worker.process.pid] = worker;
    workerLoad[worker.process.pid] = 0;

    // Listen to updates from workers
    worker.on("message", (msg) => {
      if (msg.type === "completed") {
        workerLoad[worker.process.pid] -= msg.processingTime;
        if (workerLoad[worker.process.pid] < 0) {
          workerLoad[worker.process.pid] = 0;
        }
        console.log(`✅ Worker ${worker.process.pid} completed task (${msg.processingTime}ms). Load now: ${workerLoad[worker.process.pid]}ms`);
      }
    });
  }

  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    if (req.url === "/load") {
      const expectedTime = Math.floor(1000 + Math.random() * 4000); // 1–5 seconds

      // Find the worker with least total load
      const leastLoadedPid = Object.keys(workerLoad).sort(
        (a, b) => workerLoad[a] - workerLoad[b]
      )[0];
      const selectedWorker = workers[leastLoadedPid];
      workerLoad[leastLoadedPid] += expectedTime;

      selectedWorker.send({
        type: "handle_request",
        processingTime: expectedTime
      });

      console.log(`📨 Assigned request (${expectedTime}ms) to Worker ${leastLoadedPid}. New Load: ${workerLoad[leastLoadedPid]}ms`);
      res.writeHead(200);
      res.end(`Assigned to Worker ${leastLoadedPid} | Expected Time: ${expectedTime}ms`);
    } else if (req.url === "/status") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(workerLoad));
    } else {
      res.writeHead(404);
      res.end("Not Found");
    }
  });

  server.listen(3000, () => {
    console.log("🌐 Master listening at http://localhost:3000");
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
