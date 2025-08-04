import loadtest from 'loadtest';

const options = {
    url: 'http://localhost:3000/load', // URL where the load is being sent
    maxRequests: 500,  // Number of requests to send
    concurrency: 20,   // Number of concurrent requests
};

loadtest.loadTest(options, function(error, result) {
    if (error) {
        console.error('Load test failed:', error);
    } else {
        console.log('Load test results:', result);
    }
});
