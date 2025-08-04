import express from "express";

const app = express();
const PORT = process.env.PORT || 3017;

// Track requests per worker
let requestCount = 0;

app.get("/load", (req, res) => {
    requestCount++;
    
    // Simulate worker processing the request
    const workerId = process.pid;

    // Notify master that a request was processed
    process.send({ type: "requestProcessed" });

    res.send(`Processed by Worker ${workerId} - Total Requests: ${requestCount}`);
});

app.listen(PORT, () => {
    console.log(`Worker ${process.pid} running on port ${PORT}`);
});
