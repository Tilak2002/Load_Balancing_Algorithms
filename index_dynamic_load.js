import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master process PID: ${process.pid}`);
    console.log(`🚀 Starting ${numCPUs} workers...`);

    const workers = {};
    const workerStatus = {}; // Track idle/busy status of each worker

    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workers[worker.process.pid] = worker;
        workerStatus[worker.process.pid] = "idle";

        // Listen for messages from workers
        worker.on("message", (msg) => {
            if (msg.type === "status") {
                workerStatus[worker.process.pid] = msg.status;
                console.log(`🔄 Worker ${worker.process.pid} status: ${msg.status}`);
            }

            if (msg.type === "completed") {
                console.log(`✅ Worker ${worker.process.pid} marked as idle`);
                workerStatus[worker.process.pid] = "idle";
            }
        });
    }

    const getIdleWorkerPid = () => {
        return Object.entries(workerStatus)
            .find(([_, status]) => status === "idle")?.[0];
    };

    const server = http.createServer((req, res) => {
        res.setHeader("Access-Control-Allow-Origin", "*");
        res.setHeader("Access-Control-Allow-Methods", "GET, POST");

        if (req.url === "/load") {
            const availablePid = getIdleWorkerPid();

            if (!availablePid) {
                res.writeHead(503);
                res.end("🚫 All workers are busy. Please try again shortly.\n");
                return;
            }

            const selectedWorker = workers[availablePid];
            workerStatus[availablePid] = "busy"; // Optimistically set to busy

            console.log(`🌐 Request sent to Worker ${availablePid}`);
            selectedWorker.send({ type: "handle_request" });

            res.writeHead(200);
            res.end(`✅ Request sent to Worker ${availablePid}\n`);
        } else if (req.url === "/status") {
            // Return status of all workers
            res.writeHead(200, { "Content-Type": "application/json" });
            res.end(JSON.stringify(workerStatus, null, 2));
        } else {
            res.writeHead(404);
            res.end("Not Found");
        }
    });

    server.listen(3000, () => {
        console.log("🌐 Master listening on http://localhost:3000");
    });

} else {
    console.log(`🛠️ Worker PID: ${process.pid}`);

    // Simulate request processing
    process.on("message", (msg) => {
        if (msg.type === "handle_request") {
            console.log(`⚙️ Worker ${process.pid} is handling a request...`);
            process.send({ type: "status", status: "busy" });

            setTimeout(() => {
                console.log(`✅ Worker ${process.pid} completed request`);
                process.send({ type: "status", status: "idle" });
                process.send({ type: "completed" });
            }, 2000);
        }
    });
}
