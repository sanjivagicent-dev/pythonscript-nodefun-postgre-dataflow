const { Client } = require("pg")
const parquet = require("parquetjs-lite")
const dayjs = require("dayjs")
const fs = require("fs-extra")
const AWS = require("aws-sdk")

// S3 client
const s3 = new AWS.S3()

// S3 bucket
const BUCKET_NAME = "your-s3-bucket-name"

// PostgreSQL config
const dbConfig = {
  user: "postgres",
  host: "127.0.0.1",
  database: "oilfield",
  port: 5433
}

// Lambda can only write to /tmp
const OUTPUT_DIR = "/tmp"

// For local testing
// const OUTPUT_DIR = "./parquet_output"

async function exportData(client) {

  await fs.ensureDir(OUTPUT_DIR)

  // get min & max date
  const res = await client.query(`
    SELECT MIN(date) as start_date, MAX(date) as end_date
    FROM well_data
  `)

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

    const fileName = `well_data_${start.format("YYYY_MM_DD")}.parquet`
    const filePath = `${OUTPUT_DIR}/${fileName}`

    const writer = await parquet.ParquetWriter.openFile(schema, filePath)

    for (const row of data.rows) {
      await writer.appendRow(row)
    }

    await writer.close()

    console.log("Saved locally:", filePath)

    /*
    ===============================
    Upload file to S3
    ===============================
    */

    // const fileStream = fs.createReadStream(filePath)

    // const uploadParams = {
    //   Bucket: BUCKET_NAME,
    //   Key: `parquet_exports/${fileName}`,
    //   Body: fileStream,
    //   ContentType: "application/octet-stream"
    // }

    // await s3.upload(uploadParams).promise()

    // console.log("Uploaded to S3:", fileName)

    /*
    ===============================
    Local testing only
    ===============================
    */

await fs.move(filePath, `./tmp/${fileName}`, { overwrite: true })

    start = chunkEnd
  }

  console.log("Export complete")
}

/*
=================================
AWS Lambda Handler
=================================
*/

exports.handler = async (event, context) => {

  const client = new Client(dbConfig)

  try {

    await client.connect()

    await exportData(client)

    await client.end()

    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Export completed and uploaded to S3"
      })
    }

  } catch (error) {

    console.error("Error:", error)

    if (client) {
      await client.end()
    }

    return {
      statusCode: 500,
      body: JSON.stringify({
        error: error.message
      })
    }

  }
}