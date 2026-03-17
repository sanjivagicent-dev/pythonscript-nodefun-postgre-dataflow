import dayjs from 'dayjs'
import parquet from 'parquetjs-lite'
import { schema } from './parquetService.js'
import { moveFile } from '../utils/fileUtils.js'
import { config } from '../config/env.js'
import { logger } from '../utils/logger.js'
import { PrismaClient } from '../generated/prisma/client.js'

const BATCH_SIZE = 5000

export const exportData = async (prisma: PrismaClient): Promise<void> => {
  logger.info('Starting well data export')

  const result = await prisma.well_data.aggregate({
    _min: { date: true },
    _max: { date: true },
  })

  if (!result._min.date || !result._max.date) {
    throw new Error('No data found in well_data')
  }

  const startDate = dayjs(result._min.date)
  const endDate = dayjs(result._max.date)

  let currentStart = startDate

  while (currentStart.isBefore(endDate)) {
    const chunkEnd = currentStart.add(30, 'day')

    const fileName = `well_data_${currentStart.format('YYYY_MM_DD')}.parquet`
    const filePath = `${config.outputDir}/${fileName}`

    logger.info(
      {
        from: currentStart.format('YYYY-MM-DD'),
        to: chunkEnd.format('YYYY-MM-DD'),
        filePath,
      },
      'Processing chunk'
    )

    const writer = await parquet.ParquetWriter.openFile(schema, filePath)

    let lastId: string | null = null
    let totalRows = 0

    while (true) {
      // ✅ Build query separately (fix TS issue)
      const query: any = {
        where: {
          date: {
            gte: currentStart.toDate(),
            lt: chunkEnd.toDate(),
          },
        },
        orderBy: {
          id: 'asc',
        },
        take: BATCH_SIZE,
      }

      if (lastId) {
        query.cursor = { id: lastId }
        query.skip = 1
      }

      const batch = await prisma.well_data.findMany(query)

      if (batch.length === 0) break

      for (const row of batch) {
        await writer.appendRow(row)
      }

      totalRows += batch.length
      lastId = batch[batch.length - 1].id

      logger.info(
        { batchSize: batch.length, totalRows },
        'Batch processed'
      )
    }

    await writer.close()

    logger.info({ fileName, totalRows }, 'Parquet file written')

    const destinationPath = `${config.localTmp}/${fileName}`
    await moveFile(filePath, destinationPath)

    logger.info(
      { source: filePath, destination: destinationPath },
      'File moved'
    )

    currentStart = chunkEnd
  }

  logger.info('Well data export completed')
}