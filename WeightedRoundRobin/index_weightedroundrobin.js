import cluster from "node:cluster"; 
import http from "http"; 
import os from "os";

const numCPUs = 4;
// Define weights for each worker (higher weight = more requests)
const workerWeights = {};

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);
    
    const workers = {};
    const workerStatus = {}; // Track idle/busy status
    let currentWorkerIndex = 0;
    let currentWorkerWeightCount = 0;
    
    // Create workers with different weights
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workers[worker.process.pid] = worker;
        workerStatus[worker.process.pid] = "idle";
        
        // Assign different weights based on worker number (simulating different capacities)
        // Worker 1: weight 1, Worker 2: weight 2, Worker 3: weight 3, Worker 4: weight 4
        workerWeights[worker.process.pid] = i + 1;
        
        worker.on("message", (msg) => {
            if (msg.type === "status") {
                workerStatus[worker.process.pid] = msg.status;
                console.log(`🔄 Worker ${worker.process.pid} status: ${msg.status}`);
            }
            
            if (msg.type === "completed") {
                console.log(`✅ Worker ${worker.process.pid} marked as idle`);
                workerStatus[worker.process.pid] = "idle";
            }
        });
    }
    
    // Get next worker based on weighted round robin
    const getNextWorker = () => {
        const workerPids = Object.keys(workers);
        
        // Skip to next worker if current one is busy
        let selectedPid = null;
        let loopProtection = 0;
        
        // Keep looking until we find an idle worker or exhausted all options
        while (selectedPid === null && loopProtection < workerPids.length * 2) {
            loopProtection++;
            
            // Get current worker PID
            const currentPid = workerPids[currentWorkerIndex];
            
            // Check if we've used up this worker's weight allocation
            if (currentWorkerWeightCount < workerWeights[currentPid]) {
                // Worker still has weight capacity and is idle
                if (workerStatus[currentPid] === "idle") {
                    currentWorkerWeightCount++;
                    selectedPid = currentPid;
                } else {
                    // Worker is busy, try the next one
                    currentWorkerIndex = (currentWorkerIndex + 1) % workerPids.length;
                    currentWorkerWeightCount = 0;
                }
            } else {
                // Move to next worker
                currentWorkerIndex = (currentWorkerIndex + 1) % workerPids.length;
                currentWorkerWeightCount = 0;
            }
        }
        
        return selectedPid;
    };
    
    const server = http.createServer((req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST");
        
        if (req.url === "/load") {
            const selectedPid = getNextWorker();
            
            if (!selectedPid) {
                res.writeHead(503);
                res.end("🚫 All workers are busy. Please try again shortly.\n");
                return;
            }
            
            const selectedWorker = workers[selectedPid];
            workerStatus[selectedPid] = "busy";
            
            console.log(`🌐 Request sent to Worker ${selectedPid} (Weight: ${workerWeights[selectedPid]})`);
            selectedWorker.send({ type: "handle_request" });
            
            res.writeHead(200);
            res.end(`✅ Request sent to Worker ${selectedPid}\n`);
        } else if (req.url === "/status") {
            // Also return worker weights in the status
            const statusWithWeights = {};
            Object.keys(workerStatus).forEach(pid => {
                statusWithWeights[pid] = {
                    status: workerStatus[pid],
                    weight: workerWeights[pid]
                };
            });
            
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(statusWithWeights, null, 2));
        } else {
            res.writeHead(404);
            res.end("Not Found");
        }
    });
    
    server.listen(3000, () => {
        console.log("🌐 Master listening on http://localhost:3000");
    });
} else {
    console.log(`🛠️ Worker PID: ${process.pid}`);
    
    // Simulate request processing
    process.on("message", (msg) => {
        if (msg.type === "handle_request") {
            console.log(`⚙️ Worker ${process.pid} is handling a request...`);
            process.send({ type: "status", status: "busy" });
            
            setTimeout(() => {
                console.log(`✅ Worker ${process.pid} completed request`);
                process.send({ type: "status", status: "idle" });
                process.send({ type: "completed" });
            }, 2000);
        }
    });
}