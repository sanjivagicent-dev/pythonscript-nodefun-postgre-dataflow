import { PrismaClient } from '../generated/prisma/client.js'
import { exportData } from '../services/exportService.js'

export const handler = async (): Promise<{
  statusCode: number
  body: string
}> => {
  const prisma = new PrismaClient() // ✅ moved inside

  try {
    await exportData(prisma)

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
    await prisma.$disconnect()
  }
}