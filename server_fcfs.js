// FIRST-COME, FIRST-SERVED (FCFS) LOAD BALANCER
import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    const workerBusy = {}; // Track if workers are busy
    const requestQueue = []; // FCFS queue for pending requests
    
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
                
                // Process next request in queue if any
                processNextRequest();
            }
        });
    }

    // Process next request in queue
    function processNextRequest() {
        // If no pending requests, do nothing
        if (requestQueue.length === 0) {
            return;
        }
        
        // Find an available worker
        let availableWorker = null;
        for (const [pid, busy] of Object.entries(workerBusy)) {
            if (!busy) {
                availableWorker = pid;
                break;
            }
        }
        
        // If no available worker, wait for one to become available
        if (!availableWorker) {
            return;
        }
        
        // Get the oldest request from the queue (FCFS)
        const nextRequest = requestQueue.shift();
        
        // Mark worker as busy
        workerBusy[availableWorker] = true;
        
        // Tell worker to process the request
        console.log(`🌐 Queued request from ${nextRequest.clientIP} routed to Worker ${availableWorker} (FCFS)`);
        workers[availableWorker].send({ 
            type: "handle_request",
            clientIP: nextRequest.clientIP
        });
        
        // Send response to client
        nextRequest.res.writeHead(200);
        nextRequest.res.end(`✅ Request processed by Worker ${availableWorker} (FCFS)\n`);
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
        console.log(`⚡ [${timestamp}] Request from client IP: ${clientIP}, URL: ${req.url}`);

        if (req.url === "/load") {
            // Add request to the queue with timestamp
            const request = {
                clientIP,
                res,
                timestamp: Date.now()
            };
            
            requestQueue.push(request);
            console.log(`📥 Request from ${clientIP} added to queue, position: ${requestQueue.length}`);
            
            // Try to process the request immediately if workers are available
            processNextRequest();
            
        } else if (req.url === "/status") {
            // Return status of all workers and queue
            const workersStatus = {};
            
            for (const pid in workers) {
                workersStatus[pid] = {
                    busy: workerBusy[pid]
                };
            }
            
            const status = {
                workers: workersStatus,
                queueLength: requestQueue.length,
                algorithm: "First-Come, First-Served (FCFS)"
            };
            
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(status, null, 2));
        } else {
            res.writeHead(404);
            res.end("Not Found");
        }
    });

    // Listen on all network interfaces
    server.listen(3000, "0.0.0.0", () => {
        console.log(`🌐 FCFS Load Balancer listening on http://0.0.0.0:3000`);
        console.log(`📝 Server IP: ${getLocalIpAddress()}`);
    });

    // Function to get the server's local IP address
    function getLocalIpAddress() {
        const interfaces = os.networkInterfaces();
        for (const name of Object.keys(interfaces)) {
            for (const iface of interfaces[name]) {
                if (iface.family === 'IPv4' && !iface.internal) {
                    return iface.address;
                }
            }
        }
        return '127.0.0.1';
    }

    // Handle worker exits and restart them
    cluster.on("exit", (worker, code, signal) => {
        console.log(`⚠️ Worker ${worker.process.pid} died, restarting...`);
        
        // Clean up the old worker
        delete workers[worker.process.pid];
        delete workerBusy[worker.process.pid];
        
        // Start a new worker
        const newWorker = cluster.fork();
        const newPid = newWorker.process.pid;
        workers[newPid] = newWorker;
        workerBusy[newPid] = false;
        
        // Set up message listener for new worker
        newWorker.on("message", (msg) => {
            if (msg.type === "task_completed") {
                workerBusy[newPid] = false;
                processNextRequest();
            }
        });
    });
} else {
    // Worker processes
    console.log(`👷 Worker ${process.pid} started`);
    
    // Listen for messages from master
    process.on("message", (msg) => {
        if (msg.type === "handle_request") {
            const clientIP = msg.clientIP || 'unknown';
            
            // Simulate processing time (1-5 seconds)
            const processingTime = 1000 + Math.random() * 4000;
            
            console.log(`🔧 Worker ${process.pid} processing request from ${clientIP} for ${processingTime.toFixed(0)}ms`);
            
            setTimeout(() => {
                // Notify master task is completed
                process.send({
                    type: "task_completed"
                });
                
                console.log(`✅ Worker ${process.pid} completed task from ${clientIP}`);
            }, processingTime);
        }
    });
}