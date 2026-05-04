import parquet from 'parquetjs-lite'
import { logger } from '../utils/logger.js'

logger.info('Initializing Parquet schemas for export')

/**
 * WELL DATA SCHEMA
 * Matches Prisma model: well_data  (v4 — includes predictive maintenance columns)
 */
export const wellSchema = new parquet.ParquetSchema({
  // ── Core identifiers ──────────────────────────────────────────────────────
  date:        { type: 'TIMESTAMP_MILLIS' },
  asset_id:    { type: 'UTF8' },
  facility_id: { type: 'UTF8' },

  // ── True production ───────────────────────────────────────────────────────
  true_oil:   { type: 'DOUBLE', optional: true },
  true_water: { type: 'DOUBLE', optional: true },
  true_gas:   { type: 'DOUBLE', optional: true },

  // ── Test data ─────────────────────────────────────────────────────────────
  test_oil:   { type: 'DOUBLE', optional: true },
  test_water: { type: 'DOUBLE', optional: true },
  test_gas:   { type: 'DOUBLE', optional: true },

  // ── KPIs ──────────────────────────────────────────────────────────────────
  avg_tubing_pressure:  { type: 'DOUBLE', optional: true },
  avg_casing_pressure:  { type: 'DOUBLE', optional: true },
  avg_motor_amps:       { type: 'DOUBLE', optional: true },
  gross_stroke_len:     { type: 'DOUBLE', optional: true },
  net_stroke_len:       { type: 'DOUBLE', optional: true },
  spm:                  { type: 'DOUBLE', optional: true },
  pump_fillage_pct:     { type: 'DOUBLE', optional: true },
  freq_hz:              { type: 'DOUBLE', optional: true },
  pump_intake_pressure: { type: 'DOUBLE', optional: true },
  motor_temp_f:         { type: 'DOUBLE', optional: true },
  injection_rate_mcf:   { type: 'DOUBLE', optional: true },

  // ── Status ────────────────────────────────────────────────────────────────
  status: { type: 'UTF8', optional: true },
  notes:  { type: 'UTF8', optional: true },

  // ── Predictive maintenance — phase & label ────────────────────────────────
  // "Normal" | "Pre-Failure" | "Failure" | "Post-Failure"
  degradation_phase: { type: 'UTF8', optional: true },

  // e.g. "Parted Rods", "Scale Deposition", "Liquid Loading"
  failure_mode: { type: 'UTF8', optional: true },

  // Countdown (days) to the failure event within the pre-failure ramp window.
  // NULL outside the ramp window.
  days_to_failure: { type: 'INT32', optional: true },

  // 0.0 – 1.0: how far along the degradation ramp this row sits.
  // 0.0 = healthy / ramp start; 1.0 = day of failure.
  pre_failure_ramp_intensity: { type: 'DOUBLE', optional: true },

  // ── Predictive maintenance — operational events ───────────────────────────
  // True when the row is a planned non-failure operational event.
  is_operational_event: { type: 'BOOLEAN', optional: true },

  // Human-readable event label e.g. "Choke Adjustment", "Frequency Ramp".
  event_type: { type: 'UTF8', optional: true },

  // ── Predictive maintenance — post-failure recovery ────────────────────────
  // Day number within the recovery ramp after a workover (1, 2, 3 …).
  // NULL when not in a recovery window.
  post_failure_recovery_day: { type: 'INT32', optional: true },
})

/**
 * PRODUCTION HISTORY SCHEMA
 * Matches Prisma model: production_history
 */
export const productionSchema = new parquet.ParquetSchema({
  asset_id:     { type: 'UTF8', optional: true },
  date:         { type: 'TIMESTAMP_MILLIS', optional: true },
  allocated_oil: { type: 'DOUBLE', optional: true },
  motor_temp_f: { type: 'DOUBLE', optional: true },
  status:       { type: 'UTF8', optional: true },
  notes:        { type: 'UTF8', optional: true },
})

/**
 * INVOICES SCHEMA
 * Matches Prisma model: invoices
 */
export const invoiceSchema = new parquet.ParquetSchema({
  invoice_id:          { type: 'UTF8', optional: true },
  invoice_date:        { type: 'TIMESTAMP_MILLIS', optional: true },
  category:            { type: 'UTF8', optional: true },
  gl_code:             { type: 'UTF8', optional: true },
  service_description: { type: 'UTF8', optional: true },
  vendor:              { type: 'UTF8', optional: true },
  asset_id:            { type: 'UTF8', optional: true },
  total_usd:           { type: 'DOUBLE', optional: true },
})

logger.info(
  {
    wellColumns:        Object.keys(wellSchema.fields).length,
    productionColumns:  Object.keys(productionSchema.fields).length,
    invoiceColumns:     Object.keys(invoiceSchema.fields).length,
  },
  'Parquet schemas initialized successfully'
)