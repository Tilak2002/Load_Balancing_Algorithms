import cluster from "cluster";
import os from "os";
import express from "express";

const cpuCount = os.cpus().length;
console.log(`Total CPU count: ${cpuCount}`);
console.log(`Parent Process ID: ${process.pid}`);

if (cluster.isPrimary) {
    const workerLastProcessedTime = {};

    // Fork worker processes
    for (let i = 0; i < cpuCount; i++) {
        const worker = cluster.fork();
        workerLastProcessedTime[worker.process.pid] = 0; // Initialize timestamps
    }

    // Handle worker exits and restart them
    cluster.on("exit", (worker) => {
        console.log(`Worker ${worker.process.pid} exited. Restarting...`);
        const newWorker = cluster.fork();
        workerLastProcessedTime[newWorker.process.pid] = Date.now();
    });

    // Master process sets up Express
    const app = express();
    const PORT = 3000;

    app.get("/load", (req, res) => {
        // Find the least recently used worker
        const leastRecentlyUsedWorkerId = Object.keys(workerLastProcessedTime).sort(
            (a, b) => workerLastProcessedTime[a] - workerLastProcessedTime[b]
        )[0];

        const worker = Object.values(cluster.workers).find(w => w.process.pid == leastRecentlyUsedWorkerId);

        if (worker) {
            worker.send({ type: "process_request" });
            workerLastProcessedTime[worker.process.pid] = Date.now(); // Update timestamp

            worker.once("message", (message) => {
                console.log(`Received message from Worker ${worker.process.pid}:`, message);
                if (message.type === "response") {
                    res.send(`Processed by Worker ${worker.process.pid}\n`);
                }
            });
        } else {
            res.status(500).send("No available workers.");
        }
    });

    app.listen(PORT, () => {
        console.log(`Master process listening on port ${PORT}`);
    });

} else {
    import("./fcfs.js"); // Import worker logic when a worker is forked
}
