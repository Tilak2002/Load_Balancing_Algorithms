// Refactored FCFS Load Balancer - Node.js Backend
import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
  console.log(`\n🧠 Master process PID: ${process.pid}`);
  console.log(`🚀 Starting ${numCPUs} workers...`);

  const workers = {};
  const workerStatus = {}; // Track each worker's status
  const requestQueue = []; // FCFS queue

  // Fork workers
  for (let i = 0; i < numCPUs; i++) {
    const worker = cluster.fork();
    workers[worker.process.pid] = worker;
    workerStatus[worker.process.pid] = "idle";

    // Handle messages from workers
    worker.on("message", (msg) => {
      if (msg.type === "status") {
        workerStatus[worker.process.pid] = msg.status;
        broadcastQueueStatus();
      }

      if (msg.type === "completed") {
        workerStatus[worker.process.pid] = "idle";
        console.log(`✅ Worker ${worker.process.pid} is now idle.`);
        processNextRequest();
        broadcastQueueStatus();
      }
    });
  }

  function processNextRequest() {
    const availablePid = Object.entries(workerStatus)
      .find(([_, status]) => status === "idle")?.[0];

    if (availablePid && requestQueue.length > 0) {
      const next = requestQueue.shift();
      const worker = workers[availablePid];
      workerStatus[availablePid] = "busy";

      console.log(`📤 Sending request ${next.id} to Worker ${availablePid}`);
      worker.send({ type: "handle_request", id: next.id });

      // Respond to client immediately
      next.res.writeHead(200);
      next.res.end(`✅ Request ${next.id} fully processed`);
    }
  }

  function broadcastQueueStatus() {
    // Optional: expose queue info to frontend if needed
  }

  let requestCounter = 0;

  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");

    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    if (req.url === "/load") {
      requestCounter++;
      const requestId = `req-${requestCounter}`;
      requestQueue.push({ id: requestId, req, res });
      console.log(`📥 Request ${requestId} added to queue. Length: ${requestQueue.length}`);

      processNextRequest();
    } else if (req.url === "/status") {
      const status = {
        workers: workerStatus,
        queueLength: requestQueue.length
      };
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(status, null, 2));
    } else {
      res.writeHead(404);
      res.end("Not Found");
    }
  });

  server.listen(3000, () => {
    console.log("🌐 Load Balancer running at http://localhost:3000\n");
  });

} else {
  console.log(`🛠️ Worker PID: ${process.pid}`);

  process.on("message", (msg) => {
    if (msg.type === "handle_request") {
      console.log(`⚙️ Worker ${process.pid} handling ${msg.id}`);
      process.send({ type: "status", status: "busy" });

      const processingTime = 1500 + Math.random() * 1500;
      setTimeout(() => {
        console.log(`✅ Worker ${process.pid} completed ${msg.id} in ${Math.round(processingTime)}ms`);
        process.send({ type: "completed" });
      }, processingTime);
    }
  });
}
