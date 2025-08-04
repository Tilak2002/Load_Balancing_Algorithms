// index_sjf_pipeline.js
import cluster from "node:cluster";
import http from "http";
import os from "os";

const STAGES = ["parse", "compute", "finalize"];
const numPipelines = 3; // number of pipelines = number of sets of all 3 stage workers

if (cluster.isPrimary) {
    console.log(`🧠 Master PID: ${process.pid}`);

    const workers = {};
    const stages = {
        parse: [],
        compute: [],
        finalize: []
    };
    const workerLoad = {}; // PID -> total load

    const activeResponses = {};
    let requestCounter = 0;
    const requestQueue = [];

    // Spawn workers and assign to stages
    for (let i = 0; i < numPipelines * STAGES.length; i++) {
        const stage = STAGES[i % STAGES.length];
        const worker = cluster.fork({ STAGE: stage });
        const pid = worker.process.pid;

        workers[pid] = worker;
        stages[stage].push(pid);
        workerLoad[pid] = 0;

        worker.on("message", (msg) => {
            const { type, fromPid, stage, data, processingTime } = msg;

            if (type === "completed") {
                workerLoad[fromPid] -= processingTime;
                if (workerLoad[fromPid] < 0) workerLoad[fromPid] = 0;

                const nextStage = getNextStage(stage);
                if (nextStage) {
                    const nextPid = getLeastLoadedWorker(nextStage);
                    const newEstimatedTime = estimateProcessingTime();
                    workerLoad[nextPid] += newEstimatedTime;

                    workers[nextPid].send({
                        type: "process",
                        stage: nextStage,
                        data,
                        estimatedTime: newEstimatedTime
                    });
                } else {
                    // Final stage, respond to client
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

    function getLeastLoadedWorker(stage) {
        return stages[stage].reduce((leastPid, pid) => {
            return workerLoad[pid] < workerLoad[leastPid] ? pid : leastPid;
        }, stages[stage][0]);
    }

    function estimateProcessingTime() {
        return Math.floor(500 + Math.random() * 2500); // 0.5–3s
    }

    function processNextRequest() {
        if (requestQueue.length > 0) {
            const first = requestQueue.shift();
            const { reqId, res } = first;
            const parsePid = getLeastLoadedWorker("parse");
            const estimatedTime = estimateProcessingTime();
            workerLoad[parsePid] += estimatedTime;

            activeResponses[reqId] = res;
            workers[parsePid].send({
                type: "process",
                stage: "parse",
                data: { reqId, payload: "payload" },
                estimatedTime
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
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(workerLoad, null, 2));
        } else {
            res.writeHead(404);
            res.end("Not Found");
        }
    });

    server.listen(3000, () => {
        console.log("🌐 Server listening at http://localhost:3000");
    });

} else {
    const stage = process.env.STAGE;
    const pid = process.pid;
    console.log(`🔧 Worker PID ${pid} started for stage: ${stage}`);

    process.on("message", (msg) => {
        if (msg.type === "process" && msg.stage === stage) {
            const { data, estimatedTime } = msg;
            console.log(`⚙️ ${stage.toUpperCase()} Worker ${pid} processing ${data.reqId} for ${estimatedTime}ms`);

            setTimeout(() => {
                const updatedData = {
                    ...data,
                    payload: `${stage}-done`
                };
                process.send({
                    type: "completed",
                    fromPid: pid,
                    stage,
                    data: updatedData,
                    processingTime: estimatedTime
                });
            }, estimatedTime);
        }
    });
}
