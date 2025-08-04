// SHORTEST JOB FIRST (SJF) LOAD BALANCER
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
    // Queue for requests with estimated job sizes
    const jobQueue = [];
    
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
                
                // Process next request in queue
                processNextRequest();
            }
        });
    }

    // Process next request based on shortest job first
    function processNextRequest() {
        if (jobQueue.length === 0) {
            return; // No pending requests
        }
        
        // Sort queue by estimated job size (smallest first)
        jobQueue.sort((a, b) => a.estimatedTime - b.estimatedTime);
        
        // Find an available worker
        let availableWorker = null;
        for (const [pid, busy] of Object.entries(workerBusy)) {
            if (!busy) {
                availableWorker = pid;
                break;
            }
        }
        
        // If no available worker, wait for next worker to become available
        if (!availableWorker) {
            return;
        }
        
        // Get the job with the smallest estimated time
        const nextRequest = jobQueue.shift();
        
        // Mark worker as busy
        workerBusy[availableWorker] = true;
        
        // Tell worker to process the request
        console.log(`🌐 Job size ${nextRequest.estimatedTime}ms from ${nextRequest.clientIP} routed to Worker ${availableWorker}`);
        workers[availableWorker].send({ 
            type: "handle_request",
            clientIP: nextRequest.clientIP,
            estimatedTime: nextRequest.estimatedTime
        });
        
        // Send response to client
        nextRequest.res.writeHead(200);
        nextRequest.res.end(`✅ Job size ${nextRequest.estimatedTime}ms processed by Worker ${availableWorker}\n`);
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
            // Get estimated time from query parameter or generate random time
            // Lower time = smaller job
            const estimatedTime = parseInt(parsedUrl.query.time) || 
                                  Math.floor(Math.random() * 2000) + 500; // Default 500-2500ms
            
            // Create request object
            const request = {
                clientIP,
                res,
                timestamp: Date.now(),
                estimatedTime
            };
            
            // Add request to job queue
            jobQueue.push(request);
            console.log(`📋 Added job with estimated time ${estimatedTime}ms to queue. Queue length: ${jobQueue.length}`);
            
            // Try to process the request immediately if workers are available
            processNextRequest();
        } else {
            // For any other path, return a simple message
            res.writeHead(200);
            res.end("⚡ SJF Load Balancer Server Running. Use /load?time=[milliseconds] to submit tasks.\n");
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
            const processingTime = msg.estimatedTime;
            
            console.log(`⏳ Worker ${process.pid} processing job size ${processingTime}ms from ${msg.clientIP}`);
            
            // Simulate task processing using the estimated time
            setTimeout(() => {
                console.log(`🏁 Worker ${process.pid} finished processing request from ${msg.clientIP}`);
                
                // Let the master know we're done
                process.send({ type: "task_completed" });
            }, processingTime);
        }
    });
}