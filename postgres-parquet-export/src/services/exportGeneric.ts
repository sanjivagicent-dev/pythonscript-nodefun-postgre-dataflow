import dayjs from 'dayjs'
import parquet from 'parquetjs-lite'
import fs from 'fs'

import { moveFile } from '../utils/fileUtils.js'
import { config } from '../config/env.js'
import { logger } from '../utils/logger.js'

const BATCH_SIZE = 5000

export const exportTable = async ({
  prisma,
  tableName,
  dateField,
  schema,
}: any): Promise<void> => {
  logger.info(`🚀 Starting export for ${tableName}`)

  const hasDateField = !!dateField

  // ─────────────────────────────────────────────────────────────
  // NON-DATED TABLES
  // Example:
  //   rb_assets
  //   rb_personnel
  // ─────────────────────────────────────────────────────────────

  if (!hasDateField) {
    const rows = await prisma[tableName].findMany()

    if (!rows.length) {
      logger.warn(`⚠️ No data found in ${tableName}`)
      return
    }

    const tableDir = `${config.outputDir}/${tableName}`

    if (!fs.existsSync(tableDir)) {
      fs.mkdirSync(tableDir, { recursive: true })
    }

    const filePath = `${tableDir}/${tableName}.parquet`

    const writer = await parquet.ParquetWriter.openFile(
      schema,
      filePath,
      {
        compression: 'SNAPPY',
      }
    )

    for (const row of rows) {
      await writer.appendRow(row)
    }

    await writer.close()

    logger.info(
      {
        tableName,
        totalRows: rows.length,
        filePath,
      },
      '✅ Export complete'
    )

    return
  }

  // ─────────────────────────────────────────────────────────────
  // DATED TABLES
  // ─────────────────────────────────────────────────────────────

  const result = await prisma[tableName].aggregate({
    _min: { [dateField]: true },
    _max: { [dateField]: true },
  })

  const minDate = result._min[dateField]
  const maxDate = result._max[dateField]

  if (!minDate || !maxDate) {
    logger.warn(`⚠️ No data found in ${tableName}`)
    return
  }

  let currentStart = dayjs(minDate)
  const endDate = dayjs(maxDate)

  while (currentStart.isBefore(endDate)) {
    const chunkEnd = currentStart.add(30, 'day')

    const year = currentStart.format('YYYY')

    const fileName =
      `${tableName}_${currentStart.format('YYYY_MM_DD')}.parquet`

    const tableDir =
      `${config.outputDir}/${tableName}/${year}`

    if (!fs.existsSync(tableDir)) {
      fs.mkdirSync(tableDir, { recursive: true })
    }

    const filePath = `${tableDir}/${fileName}`

    logger.info(
      {
        tableName,
        from: currentStart.format('YYYY-MM-DD'),
        to: chunkEnd.format('YYYY-MM-DD'),
        filePath,
      },
      '📦 Processing chunk'
    )

    const writer = await parquet.ParquetWriter.openFile(
      schema,
      filePath,
      {
        compression: 'SNAPPY',
      }
    )

    let lastId: bigint | null = null
    let totalRows = 0

    while (true) {
      const query: any = {
        where: {
          [dateField]: {
            gte: currentStart.toDate(),
            lt: chunkEnd.toDate(),
          },
        },
        orderBy: { id: 'asc' },
        take: BATCH_SIZE,
      }

      if (lastId) {
        query.cursor = { id: lastId }
        query.skip = 1
      }

      const batch = await prisma[tableName].findMany(query)

      if (batch.length === 0) {
        break
      }

      for (const row of batch) {
        await writer.appendRow({
          ...row,

          // TIMESTAMP FIX
          [dateField]: row[dateField]
            ? new Date(row[dateField])
            : null,
        })
      }

      totalRows += batch.length
      lastId = batch[batch.length - 1].id

      logger.info(
        {
          tableName,
          batchSize: batch.length,
          totalRows,
        },
        '📊 Batch processed'
      )
    }

    if (totalRows === 0) {
      logger.warn(
        { tableName, fileName },
        '⚠️ Skipping empty file'
      )

      await writer.close()

      currentStart = chunkEnd
      continue
    }

    await writer.close()

    const destinationDir =
      `${config.localTmp}/${tableName}/${year}`

    if (!fs.existsSync(destinationDir)) {
      fs.mkdirSync(destinationDir, { recursive: true })
    }

    const destinationPath =
      `${destinationDir}/${fileName}`

    await moveFile(filePath, destinationPath)

    logger.info(
      {
        tableName,
        fileName,
        totalRows,
        destinationPath,
      },
      '✅ Export complete'
    )

    currentStart = chunkEnd
  }

  logger.info(`🎯 Finished export for ${tableName}`)
}