// ROUND ROBIN LOAD BALANCER
import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    let workerIds = []; // Array to store worker PIDs in order
    let currentWorkerIndex = 0; // Index to track current worker for Round Robin

    // Initialize workers
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        const pid = worker.process.pid;
        workers[pid] = worker;
        workerIds.push(pid);

        // Listen for messages from workers
        worker.on("message", (msg) => {
            if (msg.type === "task_completed") {
                console.log(`✅ Worker ${pid} completed a task`);
            }
        });
    }

    // Round Robin: select next worker in sequence
    function getNextWorker() {
        const selectedWorker = workerIds[currentWorkerIndex];
        // Move to next worker for next request
        currentWorkerIndex = (currentWorkerIndex + 1) % workerIds.length;
        return selectedWorker;
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
            // Get next worker using Round Robin
            const selectedWorker = getNextWorker();
            
            if (selectedWorker) {
                console.log(`🌐 Request from ${clientIP} routed to Worker ${selectedWorker} (Round Robin)`);
                
                // Tell worker to process the request
                workers[selectedWorker].send({ 
                    type: "handle_request",
                    clientIP: clientIP
                });
                
                // Send response to client
                res.writeHead(200);
                res.end(`✅ Request processed by Worker ${selectedWorker} (Round Robin)\n`);
            } else {
                res.writeHead(503);
                res.end("No available workers\n");
            }
        } else if (req.url === "/status") {
            // Return status of all workers
            const workersStatus = {};
            
            for (const pid in workers) {
                workersStatus[pid] = {
                    active: workerIds.includes(Number(pid))
                };
            }
            
            const status = {
                workers: workersStatus,
                algorithm: "Round Robin",
                currentWorker: workerIds[currentWorkerIndex]
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
        console.log(`🌐 Round Robin Load Balancer listening on http://0.0.0.0:3000`);
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
        
        // Remove the old worker from the array
        workerIds = workerIds.filter(id => id !== worker.process.pid);
        delete workers[worker.process.pid];
        
        // Start a new worker
        const newWorker = cluster.fork();
        const newPid = newWorker.process.pid;
        workers[newPid] = newWorker;
        workerIds.push(newPid);
        
        // Set up message listener for new worker
        newWorker.on("message", (msg) => {
            if (msg.type === "task_completed") {
                console.log(`✅ Worker ${newPid} completed a task`);
            }
        });
    });
} else {
    // Worker processes
    console.log(`👷 Worker ${process.pid} started`);
    
    // Listen for messages from master
    process.on("message", (msg) => {
        if (msg.type === "handle_request") {
            // Get client IP from message if available
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