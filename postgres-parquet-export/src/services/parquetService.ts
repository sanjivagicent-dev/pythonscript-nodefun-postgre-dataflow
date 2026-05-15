import parquet from 'parquetjs-lite'
import { logger } from '../utils/logger.js'

logger.info('Initializing Parquet schemas for export')

// ── EXISTING: Apex Permian Digital Twin ──────────────────────────────────────

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
  degradation_phase:          { type: 'UTF8',    optional: true },
  failure_mode:               { type: 'UTF8',    optional: true },
  days_to_failure:            { type: 'INT32',   optional: true },
  pre_failure_ramp_intensity: { type: 'DOUBLE',  optional: true },

  // ── Predictive maintenance — operational events ───────────────────────────
  is_operational_event: { type: 'BOOLEAN', optional: true },
  event_type:           { type: 'UTF8',    optional: true },

  // ── Predictive maintenance — post-failure recovery ────────────────────────
  post_failure_recovery_day: { type: 'INT32', optional: true },
})

/**
 * PRODUCTION HISTORY SCHEMA
 * Matches Prisma model: production_history
 */
export const productionSchema = new parquet.ParquetSchema({
  asset_id:      { type: 'UTF8',   optional: true },
  date:          { type: 'TIMESTAMP_MILLIS', optional: true },
  allocated_oil: { type: 'DOUBLE', optional: true },
  motor_temp_f:  { type: 'DOUBLE', optional: true },
  status:        { type: 'UTF8',   optional: true },
  notes:         { type: 'UTF8',   optional: true },
})

/**
 * INVOICES SCHEMA (legacy)
 * Matches Prisma model: invoices
 */
export const invoiceSchema = new parquet.ParquetSchema({
  invoice_id:          { type: 'UTF8',           optional: true },
  invoice_date:        { type: 'TIMESTAMP_MILLIS',optional: true },
  category:            { type: 'UTF8',           optional: true },
  gl_code:             { type: 'UTF8',           optional: true },
  service_description: { type: 'UTF8',           optional: true },
  vendor:              { type: 'UTF8',           optional: true },
  asset_id:            { type: 'UTF8',           optional: true },
  total_usd:           { type: 'DOUBLE',         optional: true },
})

// ── NEW: Red Bluff Resources Digital Twin ────────────────────────────────────

/**
 * RB_ASSETS SCHEMA
 * Matches Prisma model: rb_assets
 * type: "Well" | "Facility" | "Compressor" | "SWD Well" | "Tank Battery"
 * lift: "Pumping Unit" | "ESP" | "Gas Lift" | null
 */
export const rbAssetsSchema = new parquet.ParquetSchema({
  asset_id:  { type: 'UTF8' },
  type:      { type: 'UTF8' },
  lift:      { type: 'UTF8',  optional: true },
  basin:     { type: 'UTF8',  optional: true },
  sub_basin: { type: 'UTF8',  optional: true },
  state:     { type: 'UTF8',  optional: true },
  county:    { type: 'UTF8',  optional: true },
  region:    { type: 'UTF8',  optional: true },
})

/**
 * RB_PERSONNEL SCHEMA
 * Matches Prisma model: rb_personnel
 * role_key: "prod_technician" | "well_perf_engineer"
 */
export const rbPersonnelSchema = new parquet.ParquetSchema({
  asset_id: { type: 'UTF8' },
  role_key: { type: 'UTF8' },
  name:     { type: 'UTF8' },
  role:     { type: 'UTF8' },
  email:    { type: 'UTF8' },
})

/**
 * RB_WELL_HISTORY SCHEMA
 * Matches Prisma model: rb_well_history
 * Lift-specific sensor columns are NULL for other lift types:
 *   Pumping Unit: gross_stroke_len … motor_temp_f
 *   ESP:          avg_tubing_pressure … vibration_hz
 *   Gas Lift:     casing_injection_pressure_psi … choke_position_pct
 */
export const rbWellHistorySchema = new parquet.ParquetSchema({
  asset_id: { type: 'UTF8' },
  date:     { type: 'TIMESTAMP_MILLIS' },

  // Production
  allocated_oil:   { type: 'DOUBLE', optional: true },
  test_oil:        { type: 'DOUBLE', optional: true },
  allocated_water: { type: 'DOUBLE', optional: true },
  allocated_gas:   { type: 'DOUBLE', optional: true },

  // Pumping Unit sensors
  gross_stroke_len:    { type: 'DOUBLE', optional: true },
  net_stroke_len:      { type: 'DOUBLE', optional: true },
  strokes_per_min:     { type: 'DOUBLE', optional: true },
  pump_fillage_pct:    { type: 'DOUBLE', optional: true },
  casing_pressure_psi: { type: 'DOUBLE', optional: true },
  tubing_pressure_psi: { type: 'DOUBLE', optional: true },
  motor_current_amps:  { type: 'DOUBLE', optional: true },
  freq_hz:             { type: 'DOUBLE', optional: true },
  pump_intake_pressure:{ type: 'DOUBLE', optional: true },
  motor_temp_f:        { type: 'DOUBLE', optional: true },

  // ESP sensors
  avg_tubing_pressure:         { type: 'DOUBLE', optional: true },
  avg_casing_pressure:         { type: 'DOUBLE', optional: true },
  pump_intake_pressure_psi:    { type: 'DOUBLE', optional: true },
  pump_discharge_pressure_psi: { type: 'DOUBLE', optional: true },
  vibration_hz:                { type: 'DOUBLE', optional: true },

  // Gas Lift sensors
  casing_injection_pressure_psi: { type: 'DOUBLE', optional: true },
  injection_rate_mscfd:          { type: 'DOUBLE', optional: true },
  wellhead_temp_f:               { type: 'DOUBLE', optional: true },
  choke_position_pct:            { type: 'DOUBLE', optional: true },

  run_status:    { type: 'UTF8', optional: true },
  failure_cause: { type: 'UTF8', optional: true },
})

/**
 * RB_FACILITY_HISTORY SCHEMA
 * Matches Prisma model: rb_facility_history
 * One Facility asset per lease (Chaparral, Mesquite, Caliche, Llano).
 * Captures LACT, separator tester, gas sales meter, and tank levels.
 */
export const rbFacilityHistorySchema = new parquet.ParquetSchema({
  asset_id: { type: 'UTF8' },
  date:     { type: 'TIMESTAMP_MILLIS' },

  tester_pressure_psi:     { type: 'DOUBLE', optional: true },
  tester_temp_f:           { type: 'DOUBLE', optional: true },
  tester_oil_rate_bpd:     { type: 'DOUBLE', optional: true },
  tester_water_rate_bpd:   { type: 'DOUBLE', optional: true },
  tester_gas_rate_mcfd:    { type: 'DOUBLE', optional: true },
  yesterday_oil_bbl:       { type: 'DOUBLE', optional: true },
  yesterday_water_bbl:     { type: 'DOUBLE', optional: true },
  yesterday_gas_mcf:       { type: 'DOUBLE', optional: true },
  gas_sales_rate_mscfd:    { type: 'DOUBLE', optional: true },
  gas_sales_temp_f:        { type: 'DOUBLE', optional: true },
  gas_sales_pressure_psi:  { type: 'DOUBLE', optional: true },
  lact_flow_rate_bblhr:    { type: 'DOUBLE', optional: true },
  lact_temp_f:             { type: 'DOUBLE', optional: true },
  lact_density_api:        { type: 'DOUBLE', optional: true },
  lact_bsw_pct:            { type: 'DOUBLE', optional: true },
  water_transfer_rate_bpd: { type: 'DOUBLE', optional: true },
  water_discharge_psi:     { type: 'DOUBLE', optional: true },
  water_temp_f:            { type: 'DOUBLE', optional: true },
  oil_tank_level_ft:       { type: 'DOUBLE', optional: true },
  water_tank_level_ft:     { type: 'DOUBLE', optional: true },
  run_status:              { type: 'UTF8',   optional: true },
})

/**
 * RB_COMPRESSOR_HISTORY SCHEMA
 * Matches Prisma model: rb_compressor_history
 * One Compressor asset per lease. Captures engine, suction/discharge, throughput.
 */
export const rbCompressorHistorySchema = new parquet.ParquetSchema({
  asset_id: { type: 'UTF8' },
  date:     { type: 'TIMESTAMP_MILLIS' },

  suction_pressure_psi:      { type: 'DOUBLE', optional: true },
  suction_temp_f:            { type: 'DOUBLE', optional: true },
  discharge_pressure_psi:    { type: 'DOUBLE', optional: true },
  discharge_temp_f:          { type: 'DOUBLE', optional: true },
  throughput_mscfd:          { type: 'DOUBLE', optional: true },
  engine_rpm:                { type: 'DOUBLE', optional: true },
  fuel_gas_consumption_mcfd: { type: 'DOUBLE', optional: true },
  engine_temp_f:             { type: 'DOUBLE', optional: true },
  lube_oil_pressure_psi:     { type: 'DOUBLE', optional: true },
  runtime_hrs:               { type: 'DOUBLE', optional: true },
  run_status:                { type: 'UTF8',   optional: true },
})

/**
 * RB_SWD_HISTORY SCHEMA
 * Matches Prisma model: rb_swd_history
 * One SWD Well per lease. Captures injection rates, pressures, cumulative volumes.
 */
export const rbSwdHistorySchema = new parquet.ParquetSchema({
  asset_id: { type: 'UTF8' },
  date:     { type: 'TIMESTAMP_MILLIS' },

  injection_rate_bpd:          { type: 'DOUBLE', optional: true },
  injection_pressure_psi:      { type: 'DOUBLE', optional: true },
  wellhead_pressure_psi:       { type: 'DOUBLE', optional: true },
  tubing_pressure_psi:         { type: 'DOUBLE', optional: true },
  casing_pressure_psi:         { type: 'DOUBLE', optional: true },
  injection_temp_f:            { type: 'DOUBLE', optional: true },
  daily_volume_injected_bbl:   { type: 'DOUBLE', optional: true },
  cumulative_volume_bbl:       { type: 'DOUBLE', optional: true },
  pump_discharge_pressure_psi: { type: 'DOUBLE', optional: true },
  pump_speed_hz:               { type: 'DOUBLE', optional: true },
  run_status:                  { type: 'UTF8',   optional: true },
})

/**
 * RB_TANK_BATTERY_HISTORY SCHEMA
 * Matches Prisma model: rb_tank_battery_history
 * One Tank Battery per lease. Captures tank levels, temperatures, haul-off volumes.
 * high_tank_alarm: 0 (normal) | 1 (alarm)
 */
export const rbTankBatteryHistorySchema = new parquet.ParquetSchema({
  asset_id: { type: 'UTF8' },
  date:     { type: 'TIMESTAMP_MILLIS' },

  oil_tank_level_ft:      { type: 'DOUBLE', optional: true },
  water_tank_level_ft:    { type: 'DOUBLE', optional: true },
  oil_tank_temp_f:        { type: 'DOUBLE', optional: true },
  water_tank_temp_f:      { type: 'DOUBLE', optional: true },
  oil_hauled_today_bbl:   { type: 'DOUBLE', optional: true },
  water_hauled_today_bbl: { type: 'DOUBLE', optional: true },
  high_tank_alarm:        { type: 'INT32',  optional: true },
  run_status:             { type: 'UTF8',   optional: true },
})

/**
 * RB_INVOICES SCHEMA
 * Matches Prisma model: rb_invoices
 * All financial ledger entries for Red Bluff Resources assets.
 * category: "Monthly LOE" | "Ad-Hoc LOE" | "Workover"
 */
export const rbInvoicesSchema = new parquet.ParquetSchema({
  invoice_id:          { type: 'UTF8',           optional: true },
  invoice_date:        { type: 'TIMESTAMP_MILLIS',optional: true },
  category:            { type: 'UTF8',           optional: true },
  gl_code:             { type: 'UTF8',           optional: true },
  service_description: { type: 'UTF8',           optional: true },
  vendor:              { type: 'UTF8',           optional: true },
  asset_id:            { type: 'UTF8',           optional: true },
  total_usd:           { type: 'DOUBLE',         optional: true },
})

logger.info(
  {
    // Existing schemas
    wellColumns:        Object.keys(wellSchema.fields).length,
    productionColumns:  Object.keys(productionSchema.fields).length,
    invoiceColumns:     Object.keys(invoiceSchema.fields).length,
    // Red Bluff schemas
    rbAssetsColumns:            Object.keys(rbAssetsSchema.fields).length,
    rbPersonnelColumns:         Object.keys(rbPersonnelSchema.fields).length,
    rbWellHistoryColumns:       Object.keys(rbWellHistorySchema.fields).length,
    rbFacilityHistoryColumns:   Object.keys(rbFacilityHistorySchema.fields).length,
    rbCompressorHistoryColumns: Object.keys(rbCompressorHistorySchema.fields).length,
    rbSwdHistoryColumns:        Object.keys(rbSwdHistorySchema.fields).length,
    rbTankBatteryHistoryColumns:Object.keys(rbTankBatteryHistorySchema.fields).length,
    rbInvoicesColumns:          Object.keys(rbInvoicesSchema.fields).length,
  },
  'Parquet schemas initialized successfully'
)