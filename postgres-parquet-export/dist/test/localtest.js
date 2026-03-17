import { handler } from '../handler/lambdaHandler.js';
async function run() {
    try {
        console.log('Starting local Lambda test...');
        const result = await handler(); // ✅ correct
        console.log('Result:', result);
    }
    catch (error) {
        console.error('FULL ERROR:');
        console.error(error);
        if (error instanceof Error) {
            console.error('Message:', error.message);
            console.error('Stack:', error.stack);
        }
    }
}
run();
