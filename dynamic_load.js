process.on("message", (message) => {
    if (message.type === "process_request") {
        console.log(`⚡ Worker ${process.pid} received a request`);

        const load = Math.floor(Math.random() * 100);

        // Respond to master
        if (typeof process.send === "function") {
            process.send({ type: "load_report", load });
            process.send({ type: "response" });
            console.log(`📤 Worker ${process.pid} responded with load ${load}`);
        }
    }
});

// Periodically report load
setInterval(() => {
    const load = Math.floor(Math.random() * 100);
    if (typeof process.send === "function") {
        process.send({ type: "load_report", load });
        console.log(`🔁 Worker ${process.pid} periodic load report: ${load}`);
    }
}, 10000);
