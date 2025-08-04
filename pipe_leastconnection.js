// leastconnection_pipeline.js
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

  const workerConnections = {}; // Track active connections per worker
  const activeResponses = {};
  const requestQueue = [];
  let requestCounter = 0;

  // Spawn workers and assign them to a stage
  for (let i = 0; i < numPipelines * STAGES.length; i++) {
    const stage = STAGES[i % STAGES.length];
    const worker = cluster.fork({ STAGE: stage });
    const pid = worker.process.pid;

    workers[pid] = worker;
    stages[stage].push(pid);
    workerConnections[pid] = 0;

    worker.on("message", (msg) => {
      const { type, stage, data, fromPid } = msg;

      if (type === "connection_start") {
        workerConnections[fromPid]++;
      }

      if (type === "connection_end") {
        workerConnections[fromPid] = Math.max(0, workerConnections[fromPid] - 1);
      }

      if (type === "completed") {
        const nextStage = getNextStage(stage);
        if (nextStage) {
          const nextWorkerPid = getLeastConnectionWorker(nextStage);
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

  // Helpers
  function getNextStage(stage) {
    const idx = STAGES.indexOf(stage);
    return idx < STAGES.length - 1 ? STAGES[idx + 1] : null;
  }

  function getLeastConnectionWorker(stage) {
    const pool = stages[stage];
    return pool.reduce((leastPid, pid) => {
      return workerConnections[pid] < workerConnections[leastPid] ? pid : leastPid;
    }, pool[0]);
  }

  function processNextRequest() {
    if (requestQueue.length > 0) {
      const { reqId, res } = requestQueue.shift();
      const parseWorkerPid = getLeastConnectionWorker("parse");

      activeResponses[reqId] = res;

      workers[parseWorkerPid].send({
        type: "process",
        stage: "parse",
        data: { reqId, payload: "initial" },
      });
    }
  }

  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    if (req.url === "/load") {
      const reqId = `req-${++requestCounter}`;
      requestQueue.push({ reqId, res });
      console.log(`📥 Received ${reqId}. Queue length: ${requestQueue.length}`);
      processNextRequest();
    } else if (req.url === "/status") {
      const status = {};
      for (const stage of STAGES) {
        for (const pid of stages[stage]) {
          status[pid] = {
            stage,
            activeConnections: workerConnections[pid],
          };
        }
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(status, null, 2));
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
      const processingTime = 500 + Math.random() * 2000;

      console.log(`⚙️ ${stage.toUpperCase()} Worker ${pid} handling ${data.reqId} (${processingTime.toFixed(0)}ms)`);

      process.send({ type: "connection_start", fromPid: pid });

      setTimeout(() => {
        const output = {
          ...data,
          payload: `${stage}-done`,
        };

        process.send({ type: "completed", stage, data: output, fromPid: pid });
        process.send({ type: "connection_end", fromPid: pid });
      }, processingTime);
    }
  });
}
