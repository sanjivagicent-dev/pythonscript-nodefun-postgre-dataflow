import dotenv from "dotenv"

dotenv.config()

const requiredEnv = (name: string): string => {
  const value = process.env[name]
  if (!value) {
    throw new Error(`Missing environment variable: ${name}`)
  }
  return value
}

export const config = {
  db: {
    user: requiredEnv("DB_USER"),
    host: requiredEnv("DB_HOST"),
    database: requiredEnv("DB_NAME"),
    port: Number(requiredEnv("DB_PORT"))
  },
  s3Bucket: requiredEnv("S3_BUCKET"),
  outputDir: process.env.OUTPUT_DIR || "/tmp",
  localTmp: process.env.LOCAL_TMP || "./tmp"
}