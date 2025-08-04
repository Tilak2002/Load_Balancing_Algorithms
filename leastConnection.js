// import cluster from "cluster";
// import os from "os";
// import { dirname } from "path";
// import { fileURLToPath } from "url";

// const __dirname = dirname(fileURLToPath(import.meta.url));

// const cpuCount = os.cpus().length;

// console.log(`Total CPU count: ${cpuCount}`);
// console.log(`Parent Process id: ${process.pid}`);

// cluster.setupPrimary({
//     exec: __dirname + "/index.js",
// });

// // Array to store workers
// const workers = [];

// // Fork worker processes
// for (let i = 0; i < cpuCount; i++) {
//     const worker = cluster.fork();
//     workers.push(worker);
// }

// // Function to find the worker with the least number of connections
// function findLeastLoadedWorker() {
//     let leastLoadedWorker = workers[0];
//     for (let i = 1; i < workers.length; i++) {
//         if (workers[i].getConnections() < leastLoadedWorker.getConnections()) {
//             leastLoadedWorker = workers[i];
//         }
//     }
//     return leastLoadedWorker;
// }

// // Handle worker exits
// cluster.on("exit", (worker, code, signal) => {
//     console.log(`Worker ${worker.process.pid} has been killed`);
//     console.log("Starting another worker");
//     // Find the least loaded worker and replace the dead worker
//     const leastLoadedWorker = findLeastLoadedWorker();
//     const newWorker = cluster.fork();
//     const index = workers.indexOf(worker);
//     workers[index] = newWorker;
// });

// Start your server logic in index.js

import cluster from "cluster"; 
import os from "os";
import { dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const cpuCount = os.cpus().length;

console.log(`Total CPU count: ${cpuCount}`);
console.log(`Parent Process ID: ${process.pid}`);

cluster.setupPrimary({
    exec: __dirname + "/index_least.js", // Worker process
});

// Global request counter
let totalRequestCount = 0;

// Track connections per worker
const workers = [];
const workerConnections = new Map();  // Stores workerId -> connection count

// Function to find the worker with the least number of connections
function findLeastLoadedWorker() {
    let leastLoadedWorker = null;
    let leastConnections = Infinity;

    for (const worker of workers) {
        const connections = workerConnections.get(worker.id) || 0;
        if (connections < leastConnections) {
            leastLoadedWorker = worker;
            leastConnections = connections;
        }
    }

    return leastLoadedWorker;
}

// Fork worker processes and initialize their connection count
for (let i = 0; i < cpuCount; i++) {
    const worker = cluster.fork();
    workers.push(worker);
    workerConnections.set(worker.id, 0);

    // Monitor worker's HTTP connections and update connection count
    worker.on("message", (msg) => {
        if (msg.type === "newConnection") {
            workerConnections.set(worker.id, (workerConnections.get(worker.id) || 0) + 1);
        } else if (msg.type === "endConnection") {
            workerConnections.set(worker.id, Math.max(0, (workerConnections.get(worker.id) || 0) - 1));
        } else if (msg.type === "requestProcessed") {
            totalRequestCount++; // Increment global request count
            console.log(`Total Requests Processed: ${totalRequestCount}`);
        }
    });
}

// Handle worker exits and replace with a new worker
cluster.on("exit", (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} exited. Restarting...`);
    
    const index = workers.indexOf(worker);
    if (index !== -1) {
        workers.splice(index, 1); // Remove the dead worker from the list
        workerConnections.delete(worker.id); // Remove from tracking
    }

    // Create a new worker and reinitialize tracking
    const newWorker = cluster.fork();
    workers.push(newWorker);
    workerConnections.set(newWorker.id, 0);

    newWorker.on("message", (msg) => {
        if (msg.type === "newConnection") {
            workerConnections.set(newWorker.id, (workerConnections.get(newWorker.id) || 0) + 1);
        } else if (msg.type === "endConnection") {
            workerConnections.set(newWorker.id, Math.max(0, (workerConnections.get(newWorker.id) || 0) - 1));
        } else if (msg.type === "requestProcessed") {
            totalRequestCount++; // Increment global request count
            console.log(`Total Requests Processed: ${totalRequestCount}`);
        }
    });
});

// Handle requests by assigning them to the least loaded worker
cluster.on("message", (worker, msg) => {
    if (msg.type === "request") {
        const leastLoadedWorker = findLeastLoadedWorker();
        if (leastLoadedWorker) {
            leastLoadedWorker.send(msg); // Redirect to least loaded worker
        }
    }
});



