import { handler } from '../handler/lambdaHandler.js';
describe('Lambda Handler', () => {
    it('should return success response', async () => {
        const result = await handler();
        expect(result.statusCode).toBe(200);
        const body = JSON.parse(result.body);
        expect(body.message).toBe('Export completed successfully');
    });
});
