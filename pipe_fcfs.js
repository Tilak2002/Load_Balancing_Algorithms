import cluster from "node:cluster";
import http from "http";
import os from "os";

const numPipelines = 4; // How many parallel pipelines you want
const STAGES = ["parse", "compute", "finalize"];

if (cluster.isPrimary) {
    console.log(`🧠 Master PID: ${process.pid}`);

    const workers = {};
    const stages = {
        parse: [],
        compute: [],
        finalize: []
    };

    const workerStatus = {};
    const stageQueues = {
        parse: [],
        compute: [],
        finalize: []
    };

    const activeResponses = {};
    let requestCounter = 0;

    // Spawn workers and assign stage type
    for (let i = 0; i < numPipelines * STAGES.length; i++) {
        const stageType = STAGES[i % 3];
        const worker = cluster.fork({ STAGE: stageType });
        const pid = worker.process.pid;

        workers[pid] = worker;
        stages[stageType].push(pid);
        workerStatus[pid] = "idle";

        worker.on("message", (msg) => {
            const { type, stage, data, fromPid } = msg;

            if (type === "status") {
                workerStatus[fromPid] = msg.status;
            }

            if (type === "next") {
                workerStatus[fromPid] = "idle";
                dispatchNext(stage); // check if there's more work for this stage

                const nextStage = getNextStage(stage);
                if (nextStage) {
                    enqueueStage(nextStage, data);
                } else {
                    // Final stage: respond to client
                    const res = activeResponses[data.reqId];
                    if (res) {
                        res.writeHead(200);
                        res.end(`✅ Request ${data.reqId} fully processed`);
                        delete activeResponses[data.reqId];
                    }
                }
            }
        });
    }

    // Get next stage name
    function getNextStage(stage) {
        const i = STAGES.indexOf(stage);
        return i < STAGES.length - 1 ? STAGES[i + 1] : null;
    }

    // Enqueue and try to dispatch a stage
    function enqueueStage(stage, data) {
        stageQueues[stage].push(data);
        // console.log(`📥 Received ${reqId}. Queue length: ${requestQueue.length}`);
console.log(`📊 Stage Queues → Parse: ${stageQueues.parse.length}, Compute: ${stageQueues.compute.length}, Finalize: ${stageQueues.finalize.length}`);

        dispatchNext(stage);
    }

    // Try to dispatch work to an idle worker
    function dispatchNext(stage) {
        const idlePid = stages[stage].find(pid => workerStatus[pid] === "idle");
        if (idlePid && stageQueues[stage].length > 0) {
            const data = stageQueues[stage].shift();
            workerStatus[idlePid] = "busy";
            workers[idlePid].send({ type: "process", stage, data });
            console.log(`⚙️ ${stage.toUpperCase()} Worker ${idlePid} processing ${data.reqId}`);
        }
    }

    // HTTP server
    const server = http.createServer((req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST");

        if (req.url === "/load") {
            const reqId = `req-${++requestCounter}`;
            // requestQueue.push({ req, res, reqId });
            // const reqId = `req-${++requestCounter}`;
            // console.log(`📥 Received ${reqId}.Queue length: ${requestQueue.length}`);

            activeResponses[reqId] = res;

            // Start in parse stage
            enqueueStage("parse", { reqId, payload: "start" });
        } else if (req.url === "/status") {
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify({
                workerStatus,
                stageQueueLengths: {
                    parse: stageQueues.parse.length,
                    compute: stageQueues.compute.length,
                    finalize: stageQueues.finalize.length,
                },
                activeRequests: Object.keys(activeResponses).length
            }, null, 2));
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

    console.log(`🔧 Worker ${pid} started for stage: ${stage}`);

    process.on("message", (msg) => {
        if (msg.type === "process" && msg.stage === stage) {
            const { reqId } = msg.data;
            console.log(`⚙️ ${stage.toUpperCase()} Worker ${pid} processing ${reqId}`);
            process.send({ type: "status", fromPid: pid, status: "busy" });

            const processingTime = 500 + Math.random() * 1000;
            setTimeout(() => {
                const output = { ...msg.data, payload: `${stage}-done` };
                process.send({ type: "next", stage, fromPid: pid, data: output });
            }, processingTime);
        }
    });
}
