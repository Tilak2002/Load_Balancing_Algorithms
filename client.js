// client.js
import fetch from 'node-fetch';
import readline from 'readline';

// Replace with the IP address of your server machine
const SERVER_IP = '192.168.233.168'; // Sarthak's PC IP Address
const SERVER_PORT = 3000;
const BASE_URL = `http://${SERVER_IP}:${SERVER_PORT}`;

// Create readline interface for interactive CLI
const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

// Function to send a load request
async function sendLoadRequest() {
  try {
    console.log('Sending request to server...');
    const response = await fetch(`${BASE_URL}/load`);
    const text = await response.text();
    console.log(`Response: ${text}`);
  } catch (error) {
    console.error(`Error: ${error.message}`);
  }
}

// Function to get server status
async function getServerStatus() {
  try {
    console.log('Getting server status...');
    const response = await fetch(`${BASE_URL}/status`);
    const data = await response.json();
    console.log('\nServer Status:');
    console.log(JSON.stringify(data, null, 2));
  } catch (error) {
    console.error(`Error: ${error.message}`);
  }
}

// Show menu
function showMenu() {
  console.log('\n==== Load Balancer Client ====');
  console.log('1. Send request to server');
  console.log('2. Get server status');
  console.log('3. Send multiple requests (stress test)');
  console.log('4. Exit');
  
  rl.question('Select an option: ', async (answer) => {
    switch (answer) {
      case '1':
        await sendLoadRequest();
        showMenu();
        break;
      case '2':
        await getServerStatus();
        showMenu();
        break;
      case '3':
        rl.question('How many requests to send? ', async (count) => {
          const num = parseInt(count, 10);
          console.log(`Sending ${num} requests...`);
          
          const promises = [];
          for (let i = 0; i < num; i++) {
            promises.push(sendLoadRequest());
            // Small delay to prevent overloading
            await new Promise(resolve => setTimeout(resolve, 100));
          }
          
          await Promise.all(promises);
          console.log(`Completed sending ${num} requests`);
          showMenu();
        });
        break;
      case '4':
        console.log('Exiting...');
        rl.close();
        break;
      default:
        console.log('Invalid option');
        showMenu();
    }
  });
}

// Start the client
console.log(`Connecting to server at ${BASE_URL}`);
showMenu();