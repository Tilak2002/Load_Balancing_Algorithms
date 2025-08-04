import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    const workerStatus = {}; // Track idle/busy status of each worker
    const workerConnections = {}; // Track number of active connections per worker

    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workers[worker.process.pid] = worker;
        workerStatus[worker.process.pid] = "idle";
        workerConnections[worker.process.pid] = 0;

        // Listen for messages from workers
        worker.on("message", (msg) => {
            if (msg.type === "status") {
                workerStatus[worker.process.pid] = msg.status;
                console.log(`🔄 Worker ${worker.process.pid} status: ${msg.status}`);
            }

            if (msg.type === "connection_start") {
                workerConnections[worker.process.pid]++;
                console.log(`📈 Worker ${worker.process.pid} now has ${workerConnections[worker.process.pid]} connections`);
            }

            if (msg.type === "connection_end") {
                workerConnections[worker.process.pid]--;
                console.log(`📉 Worker ${worker.process.pid} now has ${workerConnections[worker.process.pid]} connections`);
            }

            if (msg.type === "completed") {
                console.log(`✅ Worker ${worker.process.pid} marked as idle`);
                workerStatus[worker.process.pid] = "idle";
            }
        });
    }

    // Get worker with least connections
    const getLeastConnectionsWorker = () => {
        // First prioritize workers that are idle
        const idleWorkers = Object.entries(workerStatus)
            .filter(([pid, status]) => status === "idle")
            .map(([pid]) => pid);

        if (idleWorkers.length > 0) {
            // Sort idle workers by connection count
            return idleWorkers.sort((a, b) => 
                workerConnections[a] - workerConnections[b])[0];
        }

        // If no idle workers, choose the one with least connections
        const sortedWorkers = Object.keys(workerConnections)
            .sort((a, b) => workerConnections[a] - workerConnections[b]);
        
        return sortedWorkers[0];
    };

    const server = http.createServer((req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST");

        if (req.url === "/load") {
            const availablePid = getLeastConnectionsWorker();

            if (!availablePid) {
                res.writeHead(503);
                res.end("🚫 All workers are busy. Please try again shortly.\n");
                return;
            }

            const selectedWorker = workers[availablePid];
            workerStatus[availablePid] = "busy"; // Mark worker as busy

            console.log(`🌐 Request sent to Worker ${availablePid} with ${workerConnections[availablePid]} connections`);
            selectedWorker.send({ type: "handle_request" });

            res.writeHead(200);
            res.end(`✅ Request sent to Worker ${availablePid}\n`);
        } else if (req.url === "/status") {
            // Return status of all workers with connection counts
            const fullStatus = {};
            for (const pid in workerStatus) {
                fullStatus[pid] = {
                    status: workerStatus[pid],
                    connections: workerConnections[pid]
                };
            }
            
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(fullStatus, null, 2));
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
            process.send({ type: "connection_start" });

            // Simulate variable processing time between 1-3 seconds
            const processingTime = 1000 + Math.random() * 2000;
            
            setTimeout(() => {
                console.log(`✅ Worker ${process.pid} completed request in ${processingTime.toFixed(0)}ms`);
                process.send({ type: "status", status: "idle" });
                process.send({ type: "connection_end" });
                process.send({ type: "completed" });
            }, processingTime);
        }
    });
}