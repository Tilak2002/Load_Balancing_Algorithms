import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    const workerStatus = {}; // Track idle/busy status of each worker
    const requestQueue = []; // FCFS queue for requests

    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workers[worker.process.pid] = worker;
        workerStatus[worker.process.pid] = "idle";

        // Listen for messages from workers
        worker.on("message", (msg) => {
            if (msg.type === "status") {
                workerStatus[worker.process.pid] = msg.status;
                console.log(`🔄 Worker ${worker.process.pid} status: ${msg.status}`);
            }

            if (msg.type === "completed") {
                console.log(`✅ Worker ${worker.process.pid} marked as idle`);
                workerStatus[worker.process.pid] = "idle";
                
                // Check if there are pending requests in the queue
                processNextRequest();
            }
        });
    }

    // Process next request in the queue if any worker is available
    function processNextRequest() {
        if (requestQueue.length > 0) {
            const availablePid = Object.entries(workerStatus)
                .find(([_, status]) => status === "idle")?.[0];
            
            if (availablePid) {
                const nextRequest = requestQueue.shift();
                const selectedWorker = workers[availablePid];
                workerStatus[availablePid] = "busy";
                
                console.log(`🌐 Dequeued request sent to Worker ${availablePid}`);
                selectedWorker.send({ type: "handle_request" });
                
                // Send response back to client
                nextRequest.res.writeHead(200);
                nextRequest.res.end(`✅ Request processed by Worker ${availablePid}\n`);
            }
        }
    }

    const server = http.createServer((req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST");

        if (req.url === "/load") {
            // Add request to the queue (FCFS)
            requestQueue.push({ req, res });
            console.log(`📥 Request added to queue. Queue length: ${requestQueue.length}`);
            
            // Try to process immediately if possible
            processNextRequest();
            
            // If the request wasn't processed immediately, it remains in the queue
            if (requestQueue.length > 0 && requestQueue[requestQueue.length - 1].res === res) {
                console.log(`⏳ Request queued. Position: ${requestQueue.length}`);
                // We don't respond here - the response will be sent when the request is processed
            }
        } else if (req.url === "/status") {
            // Return status of all workers and queue length
            const status = {
                workers: workerStatus,
                queueLength: requestQueue.length
            };
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(status, null, 2));
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

            // Simulate work with random processing time between 1.5-3 seconds
            const processingTime = 1500 + Math.random() * 1500;
            
            setTimeout(() => {
                console.log(`✅ Worker ${process.pid} completed request after ${Math.round(processingTime)}ms`);
                process.send({ type: "status", status: "idle" });
                process.send({ type: "completed" });
            }, processingTime);
        }
    });
}