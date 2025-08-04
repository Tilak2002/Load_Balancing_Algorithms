process.on("message", (message) => {
    if (message.type === "process_request") {
        console.log(`Worker ${process.pid} processing request...`);

        let counter = 0;
        for (let i = 1; i <= 50_000_000; i++) counter++; // Simulate workload

        process.send({ type: "response" }); // Send response back to the master process
    }
});

// Tilak Test
