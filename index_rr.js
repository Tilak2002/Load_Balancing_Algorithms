import cluster from "cluster";
import os from "os";
import express from "express";

const PORT = 3017;
const cpuCount = os.cpus().length;

console.log(`Total CPU count: ${cpuCount}`);
console.log(`Parent Process ID: ${process.pid}`);

if (cluster.isPrimary) {
    // Enable round-robin scheduling (important for Windows)
    cluster.schedulingPolicy = cluster.SCHED_RR;
    
    // Fork worker processes equal to CPU cores
    for (let i = 0; i < cpuCount; i++) {
        const worker = cluster.fork();
        console.log(`Worker ${worker.process.pid} started`);
    }

    // Handle worker exits and restart them
    cluster.on("exit", (worker) => {
        console.log(`Worker ${worker.process.pid} exited. Restarting...`);
        const newWorker = cluster.fork();
        console.log(`New Worker ${newWorker.process.pid} started`);
    });
} else {
    // Worker process logic
    const app = express();

    app.get("/load", (req, res) => {
        let counter = 0;
        for (let i = 1; i <= 50_000_000; i++) counter++; // Simulate load
        res.send(`Processed by Worker ${process.pid}\n`);
    });

    app.listen(PORT, () => {
        console.log(`Worker ${process.pid} listening on port ${PORT}`);
    });
}
