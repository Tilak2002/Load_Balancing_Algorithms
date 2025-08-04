import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = os.cpus().length;
const initialWorkers = Math.max(2, Math.floor(numCPUs * 0.75)); // Start with 75% of CPUs
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

    const stageMetrics = {
        parse: { totalTime: 0, processedCount: 0, avgTime: 0 },
        compute: { totalTime: 0, processedCount: 0, avgTime: 0 },
        finalize: { totalTime: 0, processedCount: 0, avgTime: 0 }
    };

    const activeResponses = {};
    let requestCounter = 0;

    // Spawn initial workers
    for (let i = 0; i < initialWorkers * STAGES.length; i++) {
        spawnWorker(STAGES[i % STAGES.length]);
    }

    function spawnWorker(stageType) {
        const worker = cluster.fork({ STAGE: stageType });
        const pid = worker.process.pid;

        workers[pid] = worker;
        stages[stageType].push(pid);
        workerStatus[pid] = "idle";

        // console.log(`👶 Spawned new ${stageType} worker with PID: ${pid}`);

        worker.on("message", (msg) => {
            const { type, stage, data, fromPid, processingTime } = msg;

            if (type === "status") {
                workerStatus[fromPid] = msg.status;
            }

            if (type === "next") {
                // Update metrics
                if (processingTime) {
                    stageMetrics[stage].totalTime += processingTime;
                    stageMetrics[stage].processedCount++;
                    stageMetrics[stage].avgTime = stageMetrics[stage].totalTime / stageMetrics[stage].processedCount;
                }

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

                // Check if we need to rebalance workers
                checkAndRebalanceWorkers();
            }
        });

        worker.on("exit", (code, signal) => {
            console.log(`🚫 Worker ${pid} for stage ${stageType} died. Code: ${code}, Signal: ${signal}`);
            
            // Remove from tracking
            const idx = stages[stageType].indexOf(pid);
            if (idx > -1) {
                stages[stageType].splice(idx, 1);
            }
            delete workers[pid];
            delete workerStatus[pid];
            
            // Replace the worker if we still need it
            if (getLoadFactor(stageType) > 0.5) {
                console.log(`🔄 Replacing dead worker for ${stageType}`);
                spawnWorker(stageType);
            }
        });
        
        return worker;
    }

    // Get next stage name
    function getNextStage(stage) {
        const i = STAGES.indexOf(stage);
        return i < STAGES.length - 1 ? STAGES[i + 1] : null;
    }

    // Enqueue and try to dispatch a stage
    function enqueueStage(stage, data) {
        stageQueues[stage].push(data);
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

    // Calculate load factor for a stage (0-1)
    function getLoadFactor(stage) {
        if (stages[stage].length === 0) return 1; // No workers = max load
        
        // How many workers are busy + queue depth relative to workers
        const busyCount = stages[stage].filter(pid => workerStatus[pid] === "busy").length;
        const queueRatio = stageQueues[stage].length / stages[stage].length;
        
        return (busyCount / stages[stage].length) + queueRatio;
    }

    // Check which stages need more/fewer workers
    function checkAndRebalanceWorkers() {
        const loadFactors = {};
        let totalWorkers = 0;
        
        // Calculate load factors & count workers
        for (const stage of STAGES) {
            loadFactors[stage] = getLoadFactor(stage);
            totalWorkers += stages[stage].length;
        }

        console.log(`📈 Load Factors → Parse: ${loadFactors.parse.toFixed(2)}, Compute: ${loadFactors.compute.toFixed(2)}, Finalize: ${loadFactors.finalize.toFixed(2)}`);
        
        // Find bottleneck stage (highest load)
        const bottleneckStage = STAGES.reduce((a, b) => loadFactors[a] > loadFactors[b] ? a : b);
        
        // Find least loaded stage
        const leastLoadedStage = STAGES.reduce((a, b) => loadFactors[a] < loadFactors[b] ? a : b);
        
        // Only rebalance if there's a significant difference
        if (loadFactors[bottleneckStage] > 0.8 && loadFactors[leastLoadedStage] < 0.3 && 
            stages[leastLoadedStage].length > 1) {
            
            // Get an idle worker from least loaded stage
            const idleWorkerPid = stages[leastLoadedStage].find(pid => workerStatus[pid] === "idle");
            
            if (idleWorkerPid) {
                console.log(`🔄 Rebalancing: Moving worker from ${leastLoadedStage} to ${bottleneckStage}`);
                
                // End this worker and create new one for bottleneck stage
                workers[idleWorkerPid].kill();
                spawnWorker(bottleneckStage);
            }
        }
        
        // Scale up if overall system load is high
        const avgLoadFactor = STAGES.reduce((sum, stage) => sum + loadFactors[stage], 0) / STAGES.length;
        if (avgLoadFactor > 0.85 && totalWorkers < numCPUs * STAGES.length) {
            // Add worker to the bottleneck stage
            console.log(`🔼 Scaling up: Adding worker to ${bottleneckStage} stage`);
            spawnWorker(bottleneckStage);
        }
        
        // Scale down if overall system load is low
        if (avgLoadFactor < 0.2 && totalWorkers > STAGES.length * 2) {
            // Find a stage with multiple idle workers
            for (const stage of STAGES) {
                const idleWorkers = stages[stage].filter(pid => workerStatus[pid] === "idle");
                if (idleWorkers.length > 1 && stages[stage].length > 2) {
                    console.log(`🔽 Scaling down: Removing idle worker from ${stage} stage`);
                    workers[idleWorkers[0]].kill();
                    break;
                }
            }
        }
    }

    // HTTP server
    const server = http.createServer((req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST");

        if (req.url === "/load") {
            const reqId = `req-${++requestCounter}`;
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
                activeRequests: Object.keys(activeResponses).length,
                workerDistribution: {
                    parse: stages.parse.length,
                    compute: stages.compute.length,
                    finalize: stages.finalize.length
                },
                metrics: stageMetrics,
                totalWorkers: Object.keys(workers).length
            }, null, 2));
        } else if (req.url === "/scale/up") {
            // Manually add workers (one per stage)
            for (const stage of STAGES) {
                spawnWorker(stage);
            }
            res.writeHead(200);
            res.end("Added workers to all stages");
        } else if (req.url === "/scale/down") {
            // Remove idle workers (one per stage if available)
            for (const stage of STAGES) {
                const idleWorker = stages[stage].find(pid => workerStatus[pid] === "idle");
                if (idleWorker && stages[stage].length > 1) {
                    workers[idleWorker].kill();
                }
            }
            res.writeHead(200);
            res.end("Removed idle workers where possible");
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

            const startTime = Date.now();
            
            // Dynamic processing time based on stage
            let processingTime = 0;
            switch(stage) {
                case "parse":
                    processingTime = 300 + Math.random() * 600;
                    break;
                case "compute":
                    processingTime = 600 + Math.random() * 1500;
                    break;
                case "finalize":
                    processingTime = 200 + Math.random() * 500;
                    break;
            }
            
            setTimeout(() => {
                const actualTime = Date.now() - startTime;
                const output = { ...msg.data, payload: `${stage}-done` };
                process.send({ 
                    type: "next", 
                    stage, 
                    fromPid: pid, 
                    data: output,
                    processingTime: actualTime
                });
            }, processingTime);
        }
    });
}