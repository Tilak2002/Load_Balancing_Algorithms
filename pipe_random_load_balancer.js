// index_random_pipeline.js
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

  let requestCounter = 0;
  const activeResponses = {};
  const requestQueue = [];

  // Spawn workers and assign to stages
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
          const nextWorkerPid = getRandomWorker(nextStage);
          if (nextWorkerPid) {
            workers[nextWorkerPid].send({
              type: "process",
              stage: nextStage,
              data,
            });
          } else {
            console.warn(`⚠️ No worker found for stage ${nextStage}`);
          }
        } else {
          // Final stage: respond to client
          const res = activeResponses[data.reqId];
          if (res) {
            res.writeHead(200);
            res.end(`✅ Request ${data.reqId} completed`);
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

  function getRandomWorker(stage) {
    const pool = stages[stage];
    if (pool.length === 0) return null;
    const randomIndex = Math.floor(Math.random() * pool.length);
    return pool[randomIndex];
  }

  function processNextRequest() {
    if (requestQueue.length > 0) {
      const { reqId, res } = requestQueue.shift();
      const firstWorkerPid = getRandomWorker("parse");

      if (firstWorkerPid) {
        activeResponses[reqId] = res;
        workers[firstWorkerPid].send({
          type: "process",
          stage: "parse",
          data: { reqId, payload: "initial" },
        });
      }
    }
  }

  const server = http.createServer((req, res) => {
    res.setHeader("Access-Control-Allow-Origin", "*");

    if (req.url === "/load") {
      const reqId = `req-${++requestCounter}`;
      requestQueue.push({ reqId, res });
      console.log(`📥 Received ${reqId}. Queue length: ${requestQueue.length}`);
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
      const processingTime = Math.floor(500 + Math.random() * 2000); // 0.5–2.5s
      const { data } = msg;

      console.log(`⚙️ ${stage.toUpperCase()} Worker ${pid} processing ${data.reqId} for ${processingTime}ms`);

      setTimeout(() => {
        const output = {
          ...data,
          payload: `${stage}-done`,
        };
        process.send({ type: "completed", fromPid: pid, stage, data: output });
      }, processingTime);
    }
  });
}
