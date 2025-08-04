import cluster from "node:cluster";
import http from "http";

const numCPUs = 4;

if (cluster.isPrimary) {
  console.log(`🧠 Master PID: ${process.pid}`);
  const workers = [];

  // Create workers
  for (let i = 0; i < numCPUs; i++) {
    const worker = cluster.fork();
    workers.push(worker);
  }

  // Create server
  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    if (req.url === "/load") {
      const processingTime = Math.floor(1000 + Math.random() * 4000); // 1–5 sec

      // 🔀 Pick a random worker
      const randomIndex = Math.floor(Math.random() * workers.length);
      const selectedWorker = workers[randomIndex];

      selectedWorker.send({
        type: "handle_request",
        processingTime
      });

      console.log(`📨 Assigned request (${processingTime}ms) to Worker ${selectedWorker.process.pid}`);
      res.writeHead(200);
      res.end(`Assigned to Worker ${selectedWorker.process.pid} | Time: ${processingTime}ms`);
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
        console.log(`✅ Worker ${process.pid} completed`);
      }, processingTime);
    }
  });
}
