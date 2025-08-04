// index_iphash_pipeline.js
import cluster from "node:cluster";
import http from "http";

const STAGES = ["parse", "compute", "finalize"];
const numPipelines = 3;

if (cluster.isPrimary) {
  console.log(`🧠 Master PID: ${process.pid}`);

  const workers = {};
  const stages = {
    parse: [],
    compute: [],
    finalize: [],
  };

  const activeResponses = {};
  const requestQueue = [];
  let requestCounter = 0;

  // Create hash function for IP
  function hashIP(ip) {
    return ip.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0);
  }

  // Spawn workers and assign to stages
  for (let i = 0; i < numPipelines * STAGES.length; i++) {
    const stage = STAGES[i % STAGES.length];
    const worker = cluster.fork({ STAGE: stage });
    const pid = worker.process.pid;

    workers[pid] = worker;
    stages[stage].push(pid);

    worker.on("message", (msg) => {
      const { type, stage, data } = msg;

      if (type === "completed") {
        const nextStage = getNextStage(stage);
        if (nextStage) {
          const ipHash = hashIP(data.clientIP);
          const nextWorkerPid = getIPHashedWorker(nextStage, ipHash);
          workers[nextWorkerPid].send({
            type: "process",
            stage: nextStage,
            data,
          });
        } else {
          const res = activeResponses[data.reqId];
          if (res) {
            res.writeHead(200);
            res.end(`✅ Request ${data.reqId} fully processed`);
            delete activeResponses[data.reqId];
          }
        }

        processNextRequest();
      }
    });
  }

  function getNextStage(stage) {
    const idx = STAGES.indexOf(stage);
    return idx < STAGES.length - 1 ? STAGES[idx + 1] : null;
  }

  function getIPHashedWorker(stage, hash) {
    const pool = stages[stage];
    const index = hash % pool.length;
    return pool[index];
  }

  function processNextRequest() {
    if (requestQueue.length > 0) {
      const { reqId, res, clientIP } = requestQueue.shift();
      const ipHash = hashIP(clientIP);
      const parseWorkerPid = getIPHashedWorker("parse", ipHash);

      activeResponses[reqId] = res;
      workers[parseWorkerPid].send({
        type: "process",
        stage: "parse",
        data: { reqId, clientIP, payload: "initial" },
      });
    }
  }

  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    if (req.url === "/load") {
      const clientIP = req.socket.remoteAddress || "127.0.0.1";
      const reqId = `req-${++requestCounter}`;
            // console.log(`📥 Received ${reqId}.Queue length: ${requestQueue.length}`);

      requestQueue.push({ reqId, res, clientIP });
      console.log(`📥 Received ${reqId} from IP ${clientIP}. .Queue length: ${requestQueue.length}`);
      processNextRequest();
    } else {
      res.writeHead(404);
      res.end("Not Found");
    }
  });

  server.listen(3000, () => {
    console.log("🌐 Server running at http://localhost:3000");
  });

} else {
  const stage = process.env.STAGE;
  const pid = process.pid;

  console.log(`🔧 Worker PID ${pid} started for stage: ${stage}`);

  process.on("message", (msg) => {
    if (msg.type === "process" && msg.stage === stage) {
      const { data } = msg;
      const processingTime = Math.floor(500 + Math.random() * 2000); // Simulate random processing time

      console.log(`⚙️ ${stage.toUpperCase()} Worker ${pid} processing ${data.reqId} (${processingTime}ms)`);

      setTimeout(() => {
        const output = {
          ...data,
          payload: `${stage}-done`,
        };

        process.send({
          type: "completed",
          fromPid: pid,
          stage,
          data: output,
        });
      }, processingTime);
    }
  });
}
