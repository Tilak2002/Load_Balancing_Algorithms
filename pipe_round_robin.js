// index_roundrobin_pipeline.js
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

  const roundRobinIndex = {
    parse: 0,
    compute: 0,
    finalize: 0,
  };

  const activeResponses = {};
  const requestQueue = [];
  let requestCounter = 0;

  // Fork workers and assign to pipeline stages
  for (let i = 0; i < numPipelines * STAGES.length; i++) {
    const stage = STAGES[i % STAGES.length];
    const worker = cluster.fork({ STAGE: stage });
    const pid = worker.process.pid;

    workers[pid] = worker;
    stages[stage].push(pid);

    worker.on("message", (msg) => {
      const { type, stage, data, fromPid } = msg;

      if (type === "completed") {
        const nextStage = getNextStage(stage);
        if (nextStage) {
          const nextWorkerPid = getNextWorkerRoundRobin(nextStage);
          workers[nextWorkerPid].send({
            type: "process",
            stage: nextStage,
            data,
          });
        } else {
          // Final response to client
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

  function getNextWorkerRoundRobin(stage) {
    const pool = stages[stage];
    const index = roundRobinIndex[stage];
    const pid = pool[index];
    roundRobinIndex[stage] = (index + 1) % pool.length;
    return pid;
  }

  function processNextRequest() {
    if (requestQueue.length > 0) {
      const { reqId, res } = requestQueue.shift();
      const parseWorkerPid = getNextWorkerRoundRobin("parse");

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
      const summary = {};
      for (const stage of STAGES) {
        summary[stage] = stages[stage];
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(summary, null, 2));
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
      const processingTime = Math.floor(500 + Math.random() * 2000);

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
