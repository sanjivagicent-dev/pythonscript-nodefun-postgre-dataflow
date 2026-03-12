import dayjs from 'dayjs'
import parquet from 'parquetjs-lite'
import { schema } from './parquetService.js'
import { moveFile } from '../utils/fileUtils.js'
import { config } from '../config/env.js'
import { logger } from '../utils/logger.js'

export const exportData = async (client: any): Promise<void> => {
  logger.info('Starting well data export')

  const result = await client.query(`
    SELECT MIN(date) as start_date, MAX(date) as end_date
    FROM well_data
  `)

  const startDate = dayjs(result.rows[0].start_date)
  const endDate = dayjs(result.rows[0].end_date)

  logger.info(
    { startDate: startDate.format('YYYY-MM-DD'), endDate: endDate.format('YYYY-MM-DD') },
    'Detected export date range'
  )

  let currentStart = startDate

  while (currentStart.isBefore(endDate)) {
    const chunkEnd = currentStart.add(30, 'day')

    logger.info(
      {
        from: currentStart.format('YYYY-MM-DD'),
        to: chunkEnd.format('YYYY-MM-DD'),
      },
      'Exporting chunk'
    )

    const data = await client.query(
      `SELECT * FROM well_data WHERE date >= $1 AND date < $2`,
      [currentStart.format('YYYY-MM-DD'), chunkEnd.format('YYYY-MM-DD')]
    )

    const fileName = `well_data_${currentStart.format('YYYY_MM_DD')}.parquet`
    const filePath = `${config.outputDir}/${fileName}`

    logger.info({ filePath, rowCount: data.rows.length }, 'Writing parquet file')

    const writer = await parquet.ParquetWriter.openFile(schema, filePath)

    for (const row of data.rows) {
      await writer.appendRow(row)
    }

    await writer.close()

    logger.info({ fileName }, 'Parquet file written successfully')

    const destinationPath = `${config.localTmp}/${fileName}`

    await moveFile(filePath, destinationPath)

    logger.info(
      { source: filePath, destination: destinationPath },
      'File moved to temporary directory'
    )

    currentStart = chunkEnd
  }

  logger.info('Well data export completed')
}