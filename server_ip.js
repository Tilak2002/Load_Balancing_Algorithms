// IP HASH LOAD BALANCER
import cluster from "node:cluster";
import http from "http";
import os from "os";
import crypto from "crypto";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    const workerIds = []; // Array to store worker PIDs in order
    
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

    // Hash the client's IP address to determine which worker to use
    function getWorkerByIPHash(clientIP) {
        // Create a hash from the client IP
        const hash = crypto.createHash('md5').update(clientIP).digest('hex');
        
        // Convert first 8 characters of hash to a number and mod by worker count
        const hashValue = parseInt(hash.substring(0, 8), 16);
        const workerIndex = hashValue % workerIds.length;
        
        return workerIds[workerIndex];
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
            // Get worker based on client IP hash
            const selectedWorker = getWorkerByIPHash(clientIP);
            
            if (selectedWorker) {
                console.log(`🌐 Request from ${clientIP} routed to Worker ${selectedWorker} (IP Hash)`);
                
                // Tell worker to process the request
                workers[selectedWorker].send({ 
                    type: "handle_request",
                    clientIP: clientIP
                });
                
                // Send response to client
                res.writeHead(200);
                res.end(`✅ Request processed by Worker ${selectedWorker} (IP Hash)\n`);
            } else {
                res.writeHead(503);
                res.end("No available workers\n");
            }
        } else if (req.url === "/status") {
            // Return status of all workers
            const workersStatus = {};
            
            for (const pid of workerIds) {
                workersStatus[pid] = {
                    active: true
                };
            }
            
            const status = {
                workers: workersStatus,
                algorithm: "IP Hash"
            };
            
            // For demonstration, show which worker would handle different example IPs
            const exampleIPs = ['192.168.1.1', '10.0.0.1', '172.16.0.1'];
            status.ipExamples = {};
            
            for (const ip of exampleIPs) {
                status.ipExamples[ip] = getWorkerByIPHash(ip);
            }
            
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(status, null, 2));
        } else {
            res.writeHead(404);
            res.end("Not Found");
        }
    });

    // Listen on all network interfaces
    server.listen(3000, "0.0.0.0", () => {
        console.log(`🌐 IP Hash Load Balancer listening on http://0.0.0.0:3000`);
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
        const index = workerIds.indexOf(worker.process.pid);
        if (index > -1) {
            workerIds.splice(index, 1);
        }
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
