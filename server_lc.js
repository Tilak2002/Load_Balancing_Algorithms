// LEAST CONNECTIONS LOAD BALANCER

import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    const activeConnections = {}; // Track number of active connections per worker
    
    // Initialize workers
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        const pid = worker.process.pid;
        workers[pid] = worker;
        activeConnections[pid] = 0;

        // Listen for messages from workers
        worker.on("message", (msg) => {
            if (msg.type === "connection_started") {
                // Increment connection count
                activeConnections[pid]++;
                console.log(`📈 Worker ${pid} now has ${activeConnections[pid]} active connections`);
            }
            
            if (msg.type === "connection_ended") {
                // Decrement connection count
                activeConnections[pid]--;
                if (activeConnections[pid] < 0) activeConnections[pid] = 0;
                console.log(`📉 Worker ${pid} now has ${activeConnections[pid]} active connections`);
            }
        });
    }

    // Find the worker with the fewest active connections
    function findLeastConnectionsWorker() {
        let minConnections = Infinity;
        let selectedWorker = null;
        
        for (const [pid, connections] of Object.entries(activeConnections)) {
            if (connections < minConnections) {
                minConnections = connections;
                selectedWorker = pid;
            }
        }
        
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
            // Find worker with least connections
            const selectedWorker = findLeastConnectionsWorker();
            
            if (selectedWorker) {
                console.log(`🌐 Request from ${clientIP} routed to Worker ${selectedWorker} with ${activeConnections[selectedWorker]} connections`);
                
                // Tell worker to process the request
                workers[selectedWorker].send({ 
                    type: "handle_request",
                    clientIP: clientIP
                });
                
                // Send response to client
                res.writeHead(200);
                res.end(`✅ Request processed by Worker ${selectedWorker} with ${activeConnections[selectedWorker]} connections (Least Connections)\n`);
            } else {
                res.writeHead(503);
                res.end("No available workers\n");
            }
        } else if (req.url === "/status") {
            // Return status of all workers with their connections
            const workersStatus = {};
            
            for (const pid in workers) {
                workersStatus[pid] = {
                    connections: activeConnections[pid]
                };
            }
            
            const status = {
                workers: workersStatus,
                algorithm: "Least Connections"
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
        console.log(`🌐 Least Connections Load Balancer listening on http://0.0.0.0:3000`);
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
        delete activeConnections[worker.process.pid];
        
        // Start a new worker
        const newWorker = cluster.fork();
        const newPid = newWorker.process.pid;
        workers[newPid] = newWorker;
        activeConnections[newPid] = 0;
        
        // Set up message listener for new worker
        newWorker.on("message", (msg) => {
            if (msg.type === "connection_started") {
                activeConnections[newPid]++;
            }
            
            if (msg.type === "connection_ended") {
                activeConnections[newPid]--;
                if (activeConnections[newPid] < 0) activeConnections[newPid] = 0;
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
            
            // Notify master about new connection
            process.send({
                type: "connection_started"
            });
            
            // Simulate processing time (1-5 seconds)
            const processingTime = 1000 + Math.random() * 4000;
            
            console.log(`🔧 Worker ${process.pid} processing request from ${clientIP} for ${processingTime.toFixed(0)}ms`);
            
            setTimeout(() => {
                // Notify master connection is ended
                process.send({
                    type: "connection_ended"
                });
                
                console.log(`✅ Worker ${process.pid} completed task from ${clientIP}`);
            }, processingTime);
        }
    });
}
