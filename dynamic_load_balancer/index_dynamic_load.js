import cluster from "node:cluster";
import http from "http";
import os from "os";

const numCPUs = 8;

if (cluster.isPrimary) {
    console.log(`🧠 Master PID: ${process.pid}`);
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }
} else {
    http.createServer((req, res) => {
        if (req.url === "/load") {
            // Simulate work
            setTimeout(() => {
                res.writeHead(200);
                res.end(`✅ Handled by worker ${process.pid}\n`);
            }, 2000);
        } else {
            res.writeHead(404);
            res.end("Not Found");
        }
    }).listen(3000, () => {
        console.log(`🚀 Worker ${process.pid} is listening`);
    });
}
