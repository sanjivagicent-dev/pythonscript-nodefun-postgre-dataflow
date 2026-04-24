import parquet from 'parquetjs-lite'
import { logger } from '../utils/logger.js'

logger.info('Initializing Parquet schemas for export')

/**
 * WELL DATA SCHEMA
 * Matches Prisma model: well_data
 */
export const wellSchema = new parquet.ParquetSchema({
  // Core fields
  date: { type: 'TIMESTAMP_MILLIS' },
  asset_id: { type: 'UTF8' },
  facility_id: { type: 'UTF8' },

  // ✅ TRUE PRODUCTION
  true_oil: { type: 'DOUBLE', optional: true },
  true_water: { type: 'DOUBLE', optional: true },
  true_gas: { type: 'DOUBLE', optional: true },

  // ✅ TEST DATA
  test_oil: { type: 'DOUBLE', optional: true },
  test_water: { type: 'DOUBLE', optional: true },
  test_gas: { type: 'DOUBLE', optional: true },

  // ✅ KPIs
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

  // ✅ STATUS
  status: { type: 'UTF8', optional: true },
  notes: { type: 'UTF8', optional: true },
})

/**
 * PRODUCTION HISTORY SCHEMA
 * Matches Prisma model: production_history
 */
export const productionSchema = new parquet.ParquetSchema({
  asset_id: { type: 'UTF8', optional: true },
  date: { type: 'TIMESTAMP_MILLIS', optional: true },

  allocated_oil: { type: 'DOUBLE', optional: true },
  motor_temp_f: { type: 'DOUBLE', optional: true },

  status: { type: 'UTF8', optional: true },
  notes: { type: 'UTF8', optional: true },
})

/**
 * INVOICES SCHEMA
 * Matches Prisma model: invoices
 */
export const invoiceSchema = new parquet.ParquetSchema({
  invoice_id: { type: 'UTF8', optional: true },
  invoice_date: { type: 'TIMESTAMP_MILLIS', optional: true },

  category: { type: 'UTF8', optional: true },
  gl_code: { type: 'UTF8', optional: true },
  service_description: { type: 'UTF8', optional: true },
  vendor: { type: 'UTF8', optional: true },

  asset_id: { type: 'UTF8', optional: true },
  total_usd: { type: 'DOUBLE', optional: true },
})

logger.info(
  {
    wellColumns: Object.keys(wellSchema.fields).length,
    productionColumns: Object.keys(productionSchema.fields).length,
    invoiceColumns: Object.keys(invoiceSchema.fields).length,
  },
  'Parquet schemas initialized successfully'
)