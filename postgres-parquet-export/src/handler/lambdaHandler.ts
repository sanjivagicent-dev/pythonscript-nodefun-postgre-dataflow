import { createClient } from '../db/postgresClient.js'
import { exportData } from '../services/exportService.js'

export const handler = async (
  event: unknown,
  context: unknown
): Promise<{ statusCode: number; body: string }> => {
  const postgresClient = createClient()

  try {
    await postgresClient.connect()

    await exportData(postgresClient)

    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Export completed successfully' }),
    }
  } catch (error) {
    const errorMessage =
      error instanceof Error ? error.message : 'Unknown error occurred'

    return {
      statusCode: 500,
      body: JSON.stringify({ error: errorMessage }),
    }
  } finally {
    await postgresClient.end()
  }
}