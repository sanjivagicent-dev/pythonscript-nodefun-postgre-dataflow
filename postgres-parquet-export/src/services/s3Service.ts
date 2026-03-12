import AWS from 'aws-sdk'
import fs from 'fs'
import { config } from '../config/env.js'
import { logger } from '../utils/logger.js'

const s3 = new AWS.S3()

export const uploadToS3 = async (
  filePath: string,
  fileName: string
): Promise<void> => {
  logger.info({ filePath, fileName }, 'Starting S3 upload')

  const fileStream = fs.createReadStream(filePath)

  const params = {
    Bucket: config.s3Bucket as string,
    Key: `parquet_exports/${fileName}`,
    Body: fileStream,
  }

  try {
    const result = await s3.upload(params).promise()

    logger.info(
      {
        bucket: params.Bucket,
        key: params.Key,
        location: result.Location,
      },
      'File uploaded successfully to S3'
    )
  } catch (error) {
    logger.error(
      {
        error,
        bucket: params.Bucket,
        key: params.Key,
      },
      'Failed to upload file to S3'
    )

    throw error
  }
}