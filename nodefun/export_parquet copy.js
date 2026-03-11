const { Client } = require("pg")
const parquet = require("parquetjs-lite")
const dayjs = require("dayjs")
const fs = require("fs-extra")

// PostgreSQL connection
const client = new Client({
  user: "postgres",
  host: "127.0.0.1",
  database: "oilfield",
  port: 5433
})

const OUTPUT_DIR = "./parquet_output"

async function exportData() {

  await client.connect()

  // get min date
  const res = await client.query(
    `SELECT MIN(date) as start_date, MAX(date) as end_date FROM well_data`
  )

  let start = dayjs(res.rows[0].start_date)
  const end = dayjs(res.rows[0].end_date)

  console.log("Start:", start.format())
  console.log("End:", end.format())

  const schema = new parquet.ParquetSchema({
    date: { type: "UTF8" },
    asset_id: { type: "UTF8" },
    facility_id: { type: "UTF8" },
    allocated_oil: { type: "DOUBLE", optional: true },
    avg_tubing_pressure: { type: "DOUBLE", optional: true },
    avg_casing_pressure: { type: "DOUBLE", optional: true },
    avg_motor_amps: { type: "DOUBLE", optional: true },
    gross_stroke_len: { type: "DOUBLE", optional: true },
    net_stroke_len: { type: "DOUBLE", optional: true },
    spm: { type: "DOUBLE", optional: true },
    pump_fillage_pct: { type: "DOUBLE", optional: true },
    freq_hz: { type: "DOUBLE", optional: true },
    pump_intake_pressure: { type: "DOUBLE", optional: true },
    motor_temp_f: { type: "DOUBLE", optional: true },
    injection_rate_mcf: { type: "DOUBLE", optional: true },
    status: { type: "UTF8", optional: true },
    notes: { type: "UTF8", optional: true }
  })

  while (start.isBefore(end)) {

    const chunkEnd = start.add(30, "day")

    console.log(`Exporting ${start.format("YYYY-MM-DD")} → ${chunkEnd.format("YYYY-MM-DD")}`)

    const query = `
      SELECT *
      FROM well_data
      WHERE date >= $1 AND date < $2
      ORDER BY date
    `

    const data = await client.query(query, [
      start.format("YYYY-MM-DD"),
      chunkEnd.format("YYYY-MM-DD")
    ])

    const filePath = `${OUTPUT_DIR}/well_data_${start.format("YYYY_MM_DD")}.parquet`

    const writer = await parquet.ParquetWriter.openFile(schema, filePath)

    for (const row of data.rows) {
      await writer.appendRow(row)
    }

    await writer.close()

    console.log("Saved:", filePath)

    start = chunkEnd
  }

  await client.end()

  console.log("Export complete")
}

exportData()