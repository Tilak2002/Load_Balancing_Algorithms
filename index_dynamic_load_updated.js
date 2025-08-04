// import cluster from "node:cluster";
// import http from "http";
// import os from "os";

// const numCPUs = 8;

// if (cluster.isPrimary) {
//     console.log(`🧠 Master PID: ${process.pid}`);
//     for (let i = 0; i < numCPUs; i++) {
//         cluster.fork();
//     }
// } else {
//     http.createServer((req, res) => {
//         if (req.url === "/load") {
//             // Simulate work
//             setTimeout(() => {
//                 res.writeHead(200);
//                 res.end(`✅ Handled by worker ${process.pid}\n`);
//             }, 2000);
//         } else {
//             res.writeHead(404);
//             res.end("Not Found");
//         }
//     }).listen(3000, () => {
//         console.log(`🚀 Worker ${process.pid} is listening`);
//     });
// }


import cluster from "node:cluster";
import http from "http";
import { cpus } from "os";

const numCPUs = 4;

if (cluster.isPrimary) {
    console.log(`🧠 Master PID: ${process.pid}`);

    const workers = [];
    const workerLoad = new Map(); // { workerId: number of active requests }

    // Fork workers
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workers.push(worker);
        workerLoad.set(worker.id, 0);

        console.log(`Worker ${worker.process.pid} (ID: ${worker.id}) started`);

        // Listen for when worker finishes a request
        worker.on("message", (msg) => {
            if (msg.cmd === "done") {
                workerLoad.set(worker.id, workerLoad.get(worker.id) - 1);
                console.log(`✔️  Worker ${worker.process.pid} (ID: ${worker.id}) completed a request`);
            }
        });
    }

    // Custom load balancer server in master
    const server = http.createServer((req, res) => {
        // Find the worker with the least load
        const leastLoadedWorker = workers.reduce((minWorker, curr) => {
            const currLoad = workerLoad.get(curr.id);
            const minLoad = workerLoad.get(minWorker.id);
            return currLoad < minLoad ? curr : minWorker;
        });

        console.log(`📨 Incoming request ${req.url} routed to worker ${leastLoadedWorker.process.pid} (ID: ${leastLoadedWorker.id})`);

        // Increment load count for selected worker
        workerLoad.set(leastLoadedWorker.id, workerLoad.get(leastLoadedWorker.id) + 1);

        // Serialize the request to be passed via IPC
        const { method, url, headers } = req;
        const chunks = [];
        req.on("data", chunk => chunks.push(chunk));
        req.on("end", () => {
            const body = Buffer.concat(chunks).toString();

            const requestData = {
                cmd: "handle",
                method,
                url,
                headers,
                body
            };

            // Set up temporary response channel
            const replyChannel = `response_${process.pid}_${Date.now()}`;
            requestData.replyChannel = replyChannel;

            // Listen for response from worker
            process.once(replyChannel, (response) => {
                res.writeHead(response.statusCode, response.headers);
                res.end(response.body);
            });

            // Forward to worker
            leastLoadedWorker.send(requestData);
        });
    });

    server.listen(3000, () => {
        console.log("🔀 Load Balancer (Master) listening on port 3000");
    });

    // Setup IPC channel to receive response from workers
    for (const worker of workers) {
        worker.on("message", (msg) => {
            if (msg.replyChannel) {
                process.emit(msg.replyChannel, msg);
            }
        });
    }
} else {
    // Worker handles raw request data and sends back response
    console.log(`🔧 Worker process started: ${process.pid}`);
    process.on("message", (msg) => {
        if (msg.cmd === "handle") {
            const { method, url, headers, body, replyChannel } = msg;

            if (url === "/load") {
                setTimeout(() => {
                    console.log(`🔨 Worker ${process.pid} handled request for ${url}`);
                    const response = {
                        replyChannel,
                        statusCode: 200,
                        headers: { "Content-Type": "text/plain" },
                        body: `✅ Handled by worker ${process.pid}\n`
                    };
                    process.send(response);
                    process.send({ cmd: "done" });
                }, 2000);
            } else {
                const response = {
                    replyChannel,
                    statusCode: 404,
                    headers: { "Content-Type": "text/plain" },
                    body: "Not Found"
                };
                process.send(response);
                process.send({ cmd: "done" });
            }
        }
    });
}