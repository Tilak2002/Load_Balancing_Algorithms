// import cluster from "node:cluster";
// import http from "http";
// import os from "os";

// const numCPUs = 4;

// if (cluster.isPrimary) {
//     console.log(`🧠 Master process PID: ${process.pid}`);
//     console.log(`🚀 Starting ${numCPUs} workers...`);

//     const workers = {};
//     const workerLoads = {}; // Track load percentage of each worker
//     const activeRequests = {}; // Track active requests per worker
    
//     // Initialize workers
//     for (let i = 0; i < numCPUs; i++) {
//         const worker = cluster.fork();
//         workers[worker.process.pid] = worker;
//         workerLoads[worker.process.pid] = 0;
//         activeRequests[worker.process.pid] = 0;

//         // Listen for messages from workers
//         worker.on("message", (msg) => {
//             if (msg.type === "status_update") {
//                 // Update worker load
//                 workerLoads[worker.process.pid] = msg.load;
//                 activeRequests[worker.process.pid] = msg.activeTasks;
//                 console.log(`🔄 Worker ${worker.process.pid} load: ${msg.load}%, active tasks: ${msg.activeTasks}`);
//             }
            
//             if (msg.type === "task_completed") {
//                 // Decrement active tasks for this worker
//                 activeRequests[worker.process.pid]--;
//                 if (activeRequests[worker.process.pid] < 0) activeRequests[worker.process.pid] = 0;
                
//                 // Recalculate load percentage (0-100%)
//                 // For this demo, we'll say each active task is 25% load
//                 workerLoads[worker.process.pid] = Math.min(100, activeRequests[worker.process.pid] * 25);
//                 console.log(`✅ Worker ${worker.process.pid} completed a task, new load: ${workerLoads[worker.process.pid]}%`);
//             }
//         });
//     }

//     // Find the worker with the lowest load
//     function findLeastLoadedWorker() {
//         let minLoad = Infinity;
//         let selectedWorker = null;
        
//         for (const [pid, load] of Object.entries(workerLoads)) {
//             if (load < minLoad) {
//                 minLoad = load;
//                 selectedWorker = pid;
//             }
//         }
        
//         return selectedWorker;
//     }

//     // Server for handling HTTP requests
//     const server = http.createServer((req, res) => {
//         res.setHeader("Access-Control-Allow-Origin", "*");
//         res.setHeader("Access-Control-Allow-Methods", "GET, POST");

//         if (req.url === "/load") {
//             // Find worker with lowest load
//             const selectedWorker = findLeastLoadedWorker();
            
//             if (selectedWorker) {
//                 // Increment active tasks for this worker
//                 activeRequests[selectedWorker]++;
                
//                 // Recalculate load percentage (0-100%)
//                 // For this demo, we'll say each active task is 25% load
//                 workerLoads[selectedWorker] = Math.min(100, activeRequests[selectedWorker] * 25);
                
//                 console.log(`🌐 Request routed to Worker ${selectedWorker} with load: ${workerLoads[selectedWorker]}%`);
                
//                 // Tell worker to process the request
//                 workers[selectedWorker].send({ type: "handle_request" });
                
//                 // Send response to client
//                 res.writeHead(200);
//                 res.end(`✅ Request processed by Worker ${selectedWorker} with load: ${workerLoads[selectedWorker]}%\n`);
//             } else {
//                 res.writeHead(503);
//                 res.end("No available workers\n");
//             }
//         } else if (req.url === "/status") {
//             // Return status of all workers with their loads
//             const workersStatus = {};
            
//             for (const pid in workers) {
//                 workersStatus[pid] = {
//                     load: workerLoads[pid],
//                     activeTasks: activeRequests[pid]
//                 };
//             }
            
//             const status = {
//                 workers: workersStatus
//             };
            
//             res.writeHead(200, { "Content-Type": "application/json" });
//             res.end(JSON.stringify(status, null, 2));
//         } else {
//             res.writeHead(404);
//             res.end("Not Found");
//         }
//     });

//     // Modified to listen on all network interfaces (0.0.0.0) instead of just localhost
//     server.listen(3000, "0.0.0.0", () => {
//         console.log(`🌐 Master listening on http://0.0.0.0:3000`);
//         console.log(`📝 Server IP: ${getLocalIpAddress()}`);
//     });

//     // Function to get the server's local IP address (helpful for cross-OS communication)
//     function getLocalIpAddress() {
//         const interfaces = os.networkInterfaces();
//         for (const name of Object.keys(interfaces)) {
//             for (const iface of interfaces[name]) {
//                 // Skip over non-IPv4 and internal (loopback) addresses
//                 if (iface.family === 'IPv4' && !iface.internal) {
//                     return iface.address;
//                 }
//             }
//         }
//         return '127.0.0.1'; // Fallback to localhost if no external IP is found
//     }

//     // Handle worker exits and restart them
//     cluster.on("exit", (worker, code, signal) => {
//         console.log(`⚠️ Worker ${worker.process.pid} died, restarting...`);
        
//         // Clean up the old worker
//         delete workers[worker.process.pid];
//         delete workerLoads[worker.process.pid];
//         delete activeRequests[worker.process.pid];
        
//         // Start a new worker
//         const newWorker = cluster.fork();
//         workers[newWorker.process.pid] = newWorker;
//         workerLoads[newWorker.process.pid] = 0;
//         activeRequests[newWorker.process.pid] = 0;
        
//         // Set up message listeners for new worker
//         newWorker.on("message", (msg) => {
//             if (msg.type === "status_update") {
//                 workerLoads[newWorker.process.pid] = msg.load;
//                 activeRequests[newWorker.process.pid] = msg.activeTasks;
//             }
            
//             if (msg.type === "task_completed") {
//                 activeRequests[newWorker.process.pid]--;
//                 if (activeRequests[newWorker.process.pid] < 0) activeRequests[newWorker.process.pid] = 0;
//                 workerLoads[newWorker.process.pid] = Math.min(100, activeRequests[newWorker.process.pid] * 25);
//             }
//         });
//     });
// } else {
//     // Worker processes
//     console.log(`👷 Worker ${process.pid} started`);
    
//     let activeTasks = 0;
//     const maxTasks = 4; // Maximum number of tasks each worker can handle
    
//     // Report status to master periodically
//     setInterval(() => {
//         // Calculate load percentage (0-100%)
//         const loadPercentage = Math.min(100, (activeTasks / maxTasks) * 100);
        
//         process.send({
//             type: "status_update",
//             load: loadPercentage,
//             activeTasks: activeTasks
//         });
//     }, 5000);
    
//     // Listen for messages from master
//     process.on("message", (msg) => {
//         if (msg.type === "handle_request") {
//             activeTasks++;
            
//             // Simulate processing time (1-5 seconds)
//             const processingTime = 1000 + Math.random() * 4000;
            
//             console.log(`🔧 Worker ${process.pid} processing request for ${processingTime.toFixed(0)}ms`);
            
//             setTimeout(() => {
//                 // Task completed
//                 activeTasks--;
                
//                 // Notify master task is completed
//                 process.send({
//                     type: "task_completed"
//                 });
                
//                 console.log(`✅ Worker ${process.pid} completed task, active tasks: ${activeTasks}`);
//             }, processingTime);
//         }
//     });
// }

import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    const workerLoads = {}; // Track load percentage of each worker
    const activeRequests = {}; // Track active requests per worker
    
    // Initialize workers
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workers[worker.process.pid] = worker;
        workerLoads[worker.process.pid] = 0;
        activeRequests[worker.process.pid] = 0;

        // Listen for messages from workers
        worker.on("message", (msg) => {
            if (msg.type === "status_update") {
                // Update worker load
                workerLoads[worker.process.pid] = msg.load;
                activeRequests[worker.process.pid] = msg.activeTasks;
                console.log(`🔄 Worker ${worker.process.pid} load: ${msg.load}%, active tasks: ${msg.activeTasks}`);
            }
            
            if (msg.type === "task_completed") {
                // Decrement active tasks for this worker
                activeRequests[worker.process.pid]--;
                if (activeRequests[worker.process.pid] < 0) activeRequests[worker.process.pid] = 0;
                
                // Recalculate load percentage (0-100%)
                // For this demo, we'll say each active task is 25% load
                workerLoads[worker.process.pid] = Math.min(100, activeRequests[worker.process.pid] * 25);
                console.log(`✅ Worker ${worker.process.pid} completed a task, new load: ${workerLoads[worker.process.pid]}%`);
            }
        });
    }

    // Find the worker with the lowest load
    function findLeastLoadedWorker() {
        let minLoad = Infinity;
        let selectedWorker = null;
        
        for (const [pid, load] of Object.entries(workerLoads)) {
            if (load < minLoad) {
                minLoad = load;
                selectedWorker = pid;
            }
        }
        
        return selectedWorker;
    }

    // Get client IP address from request
    function getClientIP(req) {
        // Get IP from X-Forwarded-For header if behind a proxy
        const forwardedIps = req.headers['x-forwarded-for'];
        if (forwardedIps) {
            // X-Forwarded-For can contain multiple IPs (client, proxy1, proxy2, ...)
            // First IP is the original client
            return forwardedIps.split(',')[0].trim();
        }
        
        // Otherwise get the direct connection IP
        return req.socket.remoteAddress;
    }

    // Server for handling HTTP requests
    const server = http.createServer((req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST");

        // Get and log the client's IP address
        const clientIP = getClientIP(req);
        const timestamp = new Date().toISOString();
        console.log(`⚡ [${timestamp}] Request from client IP: ${clientIP}, URL: ${req.url}`);

        if (req.url === "/load") {
            // Find worker with lowest load
            const selectedWorker = findLeastLoadedWorker();
            
            if (selectedWorker) {
                // Increment active tasks for this worker
                activeRequests[selectedWorker]++;
                
                // Recalculate load percentage (0-100%)
                // For this demo, we'll say each active task is 25% load
                workerLoads[selectedWorker] = Math.min(100, activeRequests[selectedWorker] * 25);
                
                console.log(`🌐 Request from ${clientIP} routed to Worker ${selectedWorker} with load: ${workerLoads[selectedWorker]}%`);
                
                // Tell worker to process the request
                workers[selectedWorker].send({ 
                    type: "handle_request",
                    clientIP: clientIP
                });
                
                // Send response to client
                res.writeHead(200);
                res.end(`✅ Request processed by Worker ${selectedWorker} with load: ${workerLoads[selectedWorker]}%\n`);
            } else {
                res.writeHead(503);
                res.end("No available workers\n");
            }
        } else if (req.url === "/status") {
            // Return status of all workers with their loads
            const workersStatus = {};
            
            for (const pid in workers) {
                workersStatus[pid] = {
                    load: workerLoads[pid],
                    activeTasks: activeRequests[pid]
                };
            }
            
            const status = {
                workers: workersStatus
            };
            
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(status, null, 2));
        } else {
            res.writeHead(404);
            res.end("Not Found");
        }
    });

    // Modified to listen on all network interfaces (0.0.0.0) instead of just localhost
    server.listen(3000, "0.0.0.0", () => {
        console.log(`🌐 Master listening on http://0.0.0.0:3000`);
        console.log(`📝 Server IP: ${getLocalIpAddress()}`);
        
        // Display all available network interfaces for debugging
        console.log("\n📡 Available network interfaces:");
        const interfaces = os.networkInterfaces();
        Object.keys(interfaces).forEach(ifName => {
            interfaces[ifName].forEach(iface => {
                if (!iface.internal) {
                    console.log(`   ${ifName}: ${iface.address} (${iface.family})`);
                }
            });
        });
        console.log("\n⏳ Waiting for client connections...");
    });

    // Function to get the server's local IP address
    function getLocalIpAddress() {
        const interfaces = os.networkInterfaces();
        for (const name of Object.keys(interfaces)) {
            for (const iface of interfaces[name]) {
                // Skip over non-IPv4 and internal (loopback) addresses
                if (iface.family === 'IPv4' && !iface.internal) {
                    return iface.address;
                }
            }
        }
        return '127.0.0.1'; // Fallback to localhost if no external IP is found
    }

    // Handle worker exits and restart them
    cluster.on("exit", (worker, code, signal) => {
        console.log(`⚠️ Worker ${worker.process.pid} died, restarting...`);
        
        // Clean up the old worker
        delete workers[worker.process.pid];
        delete workerLoads[worker.process.pid];
        delete activeRequests[worker.process.pid];
        
        // Start a new worker
        const newWorker = cluster.fork();
        workers[newWorker.process.pid] = newWorker;
        workerLoads[newWorker.process.pid] = 0;
        activeRequests[newWorker.process.pid] = 0;
        
        // Set up message listeners for new worker
        newWorker.on("message", (msg) => {
            if (msg.type === "status_update") {
                workerLoads[newWorker.process.pid] = msg.load;
                activeRequests[newWorker.process.pid] = msg.activeTasks;
            }
            
            if (msg.type === "task_completed") {
                activeRequests[newWorker.process.pid]--;
                if (activeRequests[newWorker.process.pid] < 0) activeRequests[newWorker.process.pid] = 0;
                workerLoads[newWorker.process.pid] = Math.min(100, activeRequests[newWorker.process.pid] * 25);
            }
        });
    });
} else {
    // Worker processes
    console.log(`👷 Worker ${process.pid} started`);
    
    let activeTasks = 0;
    const maxTasks = 4; // Maximum number of tasks each worker can handle
    
    // Report status to master periodically
    setInterval(() => {
        // Calculate load percentage (0-100%)
        const loadPercentage = Math.min(100, (activeTasks / maxTasks) * 100);
        
        process.send({
            type: "status_update",
            load: loadPercentage,
            activeTasks: activeTasks
        });
    }, 5000);
    
    // Listen for messages from master
    process.on("message", (msg) => {
        if (msg.type === "handle_request") {
            activeTasks++;
            
            // Get client IP from message if available
            const clientIP = msg.clientIP || 'unknown';
            
            // Simulate processing time (1-5 seconds)
            const processingTime = 1000 + Math.random() * 4000;
            
            console.log(`🔧 Worker ${process.pid} processing request from ${clientIP} for ${processingTime.toFixed(0)}ms`);
            
            setTimeout(() => {
                // Task completed
                activeTasks--;
                
                // Notify master task is completed
                process.send({
                    type: "task_completed"
                });
                
                console.log(`✅ Worker ${process.pid} completed task from ${clientIP}, active tasks: ${activeTasks}`);
            }, processingTime);
        }
    });
}