import { createClient } from "../db/postgresClient.js";
import { exportData } from "../services/exportService.js";
export const handler = async (p0, p1) => {
    const client = createClient();
    try {
        await client.connect();
        await exportData(client);
        await client.end();
        return {
            statusCode: 200,
            body: JSON.stringify({ message: "Export complete" })
        };
    }
    catch (error) {
        await client.end();
        return {
            statusCode: 500,
            body: error.message
        };
    }
};
