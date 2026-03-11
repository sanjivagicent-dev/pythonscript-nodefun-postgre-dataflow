import { handler } from "../handler/lambdaHandler.js";
async function run() {
    try {
        console.log("Starting local Lambda test...");
        const result = await handler({}, {});
        console.log("Result:", result);
    }
    catch (error) {
        console.error("FULL ERROR:");
        console.error(error);
        console.error(JSON.stringify(error, null, 2));
    }
}
run();
