import parquet from "parquetjs-lite";
export const schema = new parquet.ParquetSchema({
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
});
