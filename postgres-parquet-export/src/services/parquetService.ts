import parquet from 'parquetjs-lite'
import { logger } from '../utils/logger.js'

logger.info('Initializing Parquet schema for well export')

export const wellSchema = new parquet.ParquetSchema({
  date: { type: 'TIMESTAMP_MILLIS' },  // ✅ fixed
  asset_id: { type: 'UTF8' },
  facility_id: { type: 'UTF8', optional: true },

  allocated_oil: { type: 'DOUBLE', optional: true },
  avg_tubing_pressure: { type: 'DOUBLE', optional: true },
  avg_casing_pressure: { type: 'DOUBLE', optional: true },
  avg_motor_amps: { type: 'DOUBLE', optional: true },

  gross_stroke_len: { type: 'DOUBLE', optional: true },
  net_stroke_len: { type: 'DOUBLE', optional: true },
  spm: { type: 'DOUBLE', optional: true },
  pump_fillage_pct: { type: 'DOUBLE', optional: true },

  freq_hz: { type: 'DOUBLE', optional: true },
  pump_intake_pressure: { type: 'DOUBLE', optional: true },
  motor_temp_f: { type: 'DOUBLE', optional: true },
  injection_rate_mcf: { type: 'DOUBLE', optional: true },

  status: { type: 'UTF8', optional: true },
  notes: { type: 'UTF8', optional: true },
})

export const productionSchema = new parquet.ParquetSchema({
  asset_id: { type: 'UTF8' },
  date: { type: 'TIMESTAMP_MILLIS' },
  allocated_oil: { type: 'DOUBLE', optional: true },
  motor_temp_f: { type: 'DOUBLE', optional: true },
  status: { type: 'UTF8', optional: true },
  notes: { type: 'UTF8', optional: true },
})

export const invoiceSchema = new parquet.ParquetSchema({
  invoice_id: { type: 'UTF8' },
  invoice_date: { type: 'TIMESTAMP_MILLIS' },
  category: { type: 'UTF8', optional: true },
  gl_code: { type: 'UTF8', optional: true },
  service_description: { type: 'UTF8', optional: true },
  vendor: { type: 'UTF8', optional: true },
  asset_id: { type: 'UTF8', optional: true },
  total_usd: { type: 'DOUBLE', optional: true },
})

logger.info(
  {
    columnCount: Object.keys(wellSchema.fields).length,
  },
  'Parquet schema initialized successfully'
)