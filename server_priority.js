// PRIORITY QUEUE LOAD BALANCER
import cluster from "node:cluster";
import http from "http";
import os from "os";
import url from "url";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    const workerBusy = {}; // Track if workers are busy
    // Priority queues - higher priority requests are processed first
    const highPriorityQueue = [];
    const mediumPriorityQueue = [];
    const lowPriorityQueue = [];
    
    // Initialize workers
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        const pid = worker.process.pid;
        workers[pid] = worker;
        workerBusy[pid] = false;

        // Listen for messages from workers
        worker.on("message", (msg) => {
            if (msg.type === "task_completed") {
                // Mark worker as available
                workerBusy[pid] = false;
                console.log(`✅ Worker ${pid} completed a task, now available`);
                
                // Process next request in queue based on priority
                processNextRequest();
            }
        });
    }

    // Process next request from the highest priority non-empty queue
    function processNextRequest() {
        // Check queues in priority order
        let nextRequest = null;
        let priorityLevel = "";
        
        if (highPriorityQueue.length > 0) {
            nextRequest = highPriorityQueue.shift();
            priorityLevel = "high";
        } else if (mediumPriorityQueue.length > 0) {
            nextRequest = mediumPriorityQueue.shift();
            priorityLevel = "medium";
        } else if (lowPriorityQueue.length > 0) {
            nextRequest = lowPriorityQueue.shift();
            priorityLevel = "low";
        }
        
        if (!nextRequest) {
            return; // No pending requests
        }
        
        // Find an available worker
        let availableWorker = null;
        for (const [pid, busy] of Object.entries(workerBusy)) {
            if (!busy) {
                availableWorker = pid;
                break;
            }
        }
        
        // If no available worker, put request back in appropriate queue
        if (!availableWorker) {
            if (priorityLevel === "high") {
                highPriorityQueue.unshift(nextRequest);
            } else if (priorityLevel === "medium") {
                mediumPriorityQueue.unshift(nextRequest);
            } else {
                lowPriorityQueue.unshift(nextRequest);
            }
            return;
        }
        
        // Mark worker as busy
        workerBusy[availableWorker] = true;
        
        // Tell worker to process the request
        console.log(`🌐 ${priorityLevel} priority request from ${nextRequest.clientIP} routed to Worker ${availableWorker}`);
        workers[availableWorker].send({ 
            type: "handle_request",
            clientIP: nextRequest.clientIP,
            priority: priorityLevel
        });
        
        // Send response to client
        nextRequest.res.writeHead(200);
        nextRequest.res.end(`✅ ${priorityLevel} priority request processed by Worker ${availableWorker}\n`);
    }

    // Get client IP address from request
    function getClientIP(req) {
        const forwardedIps = req.headers['x-forwarded-for'];
        if (forwardedIps) {
            return forwardedIps.split(',')[0].trim();
        }
        return req.socket.remoteAddress;
    }

    // Server for handling HTTP requests
    const server = http.createServer((req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST");

        const clientIP = getClientIP(req);
        const timestamp = new Date().toISOString();
        const parsedUrl = url.parse(req.url, true);
        console.log(`⚡ [${timestamp}] Request from client IP: ${clientIP}, URL: ${req.url}`);

        if (parsedUrl.pathname === "/load") {
            // Get priority from query parameter (default to medium)
            const priority = parsedUrl.query.priority || "medium";
            
            // Create request object
            const request = {
                clientIP,
                res,
                timestamp: Date.now(),
                priority
            };
            
            // Add request to appropriate queue
            if (priority === "high") {
                highPriorityQueue.push(request);
                console.log(`📋 Added high priority request to queue. Queue length: ${highPriorityQueue.length}`);
            } else if (priority === "medium") {
                mediumPriorityQueue.push(request);
                console.log(`📋 Added medium priority request to queue. Queue length: ${mediumPriorityQueue.length}`);
            } else {
                lowPriorityQueue.push(request);
                console.log(`📋 Added low priority request to queue. Queue length: ${lowPriorityQueue.length}`);
            }
            
            // Try to process the request immediately if workers are available
            processNextRequest();
        } else {
            // For any other path, return a simple message
            res.writeHead(200);
            res.end("⚡ Load Balancer Server Running. Use /load?priority=[high|medium|low] to submit tasks.\n");
        }
    });

    const PORT = process.env.PORT || 3000;
    server.listen(PORT, () => {
        console.log(`🔌 Master is listening on port ${PORT}`);
    });

    // Handle worker exit (terminate or crash)
    cluster.on("exit", (worker, code, signal) => {
        const pid = worker.process.pid;
        console.log(`⚠️ Worker ${pid} died with code: ${code} and signal: ${signal}`);
        delete workers[pid];
        delete workerBusy[pid];
        
        // Fork a new worker to replace the dead one
        const newWorker = cluster.fork();
        const newPid = newWorker.process.pid;
        workers[newPid] = newWorker;
        workerBusy[newPid] = false;
        console.log(`🔄 New worker ${newPid} started to replace dead worker`);
        
        // Setup message handler for new worker
        newWorker.on("message", (msg) => {
            if (msg.type === "task_completed") {
                workerBusy[newPid] = false;
                console.log(`✅ Worker ${newPid} completed a task, now available`);
                processNextRequest();
            }
        });
    });
} else {
    // Worker process code
    console.log(`👷 Worker started with PID: ${process.pid}`);
    
    // Listen for messages from master
    process.on("message", (msg) => {
        if (msg.type === "handle_request") {
            // Simulate processing time based on priority
            let processingTime;
            
            switch(msg.priority) {
                case "high":
                    processingTime = Math.floor(Math.random() * 500) + 100; // 100-600ms
                    break;
                case "medium":
                    processingTime = Math.floor(Math.random() * 1000) + 500; // 500-1500ms
                    break;
                case "low":
                    processingTime = Math.floor(Math.random() * 2000) + 1000; // 1000-3000ms
                    break;
                default:
                    processingTime = Math.floor(Math.random() * 1000) + 500; // Default: 500-1500ms
            }
            
            console.log(`⏳ Worker ${process.pid} processing ${msg.priority} priority request from ${msg.clientIP} - will take ${processingTime}ms`);
            
            // Simulate task processing
            setTimeout(() => {
                console.log(`🏁 Worker ${process.pid} finished processing request from ${msg.clientIP}`);
                
                // Let the master know we're done
                process.send({ type: "task_completed" });
            }, processingTime);
        }
    });
}