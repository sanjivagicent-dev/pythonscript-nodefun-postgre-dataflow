import dayjs from "dayjs"
import parquet from "parquetjs-lite"
import { schema } from "./parquetService.js"
import { moveFile } from "../utils/fileUtils.js"
import { config } from "../config/env.js"

export const exportData = async (client: any) => {

  const res = await client.query(`
    SELECT MIN(date) as start_date, MAX(date) as end_date
    FROM well_data
  `)

  let start = dayjs(res.rows[0].start_date)
  const end = dayjs(res.rows[0].end_date)

  while (start.isBefore(end)) {

    const chunkEnd = start.add(30, "day")

    const data = await client.query(
      `SELECT * FROM well_data WHERE date >= $1 AND date < $2`,
      [start.format("YYYY-MM-DD"), chunkEnd.format("YYYY-MM-DD")]
    )

    const fileName = `well_data_${start.format("YYYY_MM_DD")}.parquet`
    const filePath = `${config.outputDir}/${fileName}`

    const writer = await parquet.ParquetWriter.openFile(schema, filePath)

    for (const row of data.rows) {
      await writer.appendRow(row)
    }

    await writer.close()

    await moveFile(filePath, `${config.localTmp}/${fileName}`)

    start = chunkEnd
  }
}