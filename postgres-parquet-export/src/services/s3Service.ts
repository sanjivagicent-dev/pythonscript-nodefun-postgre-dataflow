import AWS from "aws-sdk"
import fs from "fs"
import { config } from "../config/env.js"

const s3 = new AWS.S3()

export const uploadToS3 = async (filePath: string, fileName: string) => {

  const fileStream = fs.createReadStream(filePath)

  const params = {
    Bucket: config.s3Bucket,
    Key: `parquet_exports/${fileName}`,
    Body: fileStream
  }

  await s3.upload(params).promise()
}