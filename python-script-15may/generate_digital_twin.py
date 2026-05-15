"""
Red Bluff Resources — Digital Twin
======================================
Simulates 60 wells (15 per lease × 4 leases) with realistic:
  - Hyperbolic decline curves (Arps)
  - Lift-type sensor normals (Pumping Unit / ESP / Gas Lift)
  - Gradual random-walk deterioration pre-failure
  - Failure events with downtime and restart

Pipeline order:
  1. python3 generate_digital_twin.py   → writes to PostgreSQL (rb_assets, rb_personnel, rb_well_history)
  2. python3 generate_invoices.py       → writes rb_invoices (well LOE + workovers)
  3. python3 build_master.py            → adds location, supporting assets, cleans 12 wells
  4. python3 export_to_parquet.py       → exports all RB tables to .parquet

Output: PostgreSQL tables rb_assets, rb_personnel, rb_well_history
"""

import numpy as np
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# ── DB connection ─────────────────────────────────────────────────────────────
DB_CONFIG = dict(dbname="oilfield", user="postgres", password="postgres",
                 host="127.0.0.1", port="5433")

# ── CONFIG ────────────────────────────────────────────────────────────────────
END_DATE        = datetime(2026, 5, 7)
COMPANY         = "Red Bluff Resources"
LEASES          = ["Chaparral", "Mesquite", "Caliche", "Llano"]
WELLS_PER_LEASE = 15

LIFT_POOL = (
    ["Pumping Unit"] * 27 +
    ["ESP"]          * 21 +
    ["Gas Lift"]     * 12
)
random.shuffle(LIFT_POOL)

def random_spud():
    bucket = random.random()
    if bucket < 0.30:
        return datetime(2012, 1, 1) + timedelta(days=random.randint(0, 730))
    elif bucket < 0.65:
        return datetime(2015, 1, 1) + timedelta(days=random.randint(0, 1095))
    elif bucket < 0.85:
        return datetime(2018, 1, 1) + timedelta(days=random.randint(0, 1095))
    else:
        return datetime(2021, 1, 1) + timedelta(days=random.randint(0, 730))

# ── FAILURE LIBRARY ───────────────────────────────────────────────────────────
FAILURES = {
    "Pumping Unit": [
        {"cause": "Parted Rod String", "window_min": 14,  "window_max": 28,  "trigger_sensor": "motor_current_amps"},
        {"cause": "Worn Pump / Tag",   "window_min": 60,  "window_max": 120, "trigger_sensor": "pump_fillage_pct"},
    ],
    "ESP": [
        {"cause": "Motor Overtemp Shutdown", "window_min": 45, "window_max": 75,  "trigger_sensor": "motor_temp_f"},
        {"cause": "Motor/Cable Failure",     "window_min": 20, "window_max": 40,  "trigger_sensor": "motor_current_amps"},
        {"cause": "Pump Cavitation",         "window_min": 10, "window_max": 21,  "trigger_sensor": "pump_intake_pressure_psi"},
    ],
    "Gas Lift": [
        {"cause": "Gas Lift Valve Failure", "window_min": 60, "window_max": 90, "trigger_sensor": "casing_injection_pressure_psi"},
        {"cause": "Mandrel Washout",        "window_min": 45, "window_max": 75, "trigger_sensor": "injection_rate_mscfd"},
    ],
}

# ── SENSOR NORMAL RANGES ──────────────────────────────────────────────────────
SENSOR_NORMALS = {
    "Pumping Unit": {
        "allocated_oil":        (1.0,    2500.0),
        "test_oil":             (0.0,    0.0),
        "gross_stroke_len":     (20.0,   600.0),
        "net_stroke_len":       (20.0,   600.0),
        "strokes_per_min":      (0.5,    12.0),
        "pump_fillage_pct":     (0.0,    100.0),
        "casing_pressure_psi":  (50.0,   5000.0),
        "tubing_pressure_psi":  (50.0,   150.0),
        "motor_current_amps":   (10.0,   200.0),
        "freq_hz":              (57.0,   62.0),
        "pump_intake_pressure": (0.0,    5000.0),
        "motor_temp_f":         (100.0,  160.0),
    },
    "ESP": {
        "allocated_oil":              (1.0,    2500.0),
        "test_oil":                   (0.0,    0.0),
        "avg_tubing_pressure":        (200.0,  500.0),
        "avg_casing_pressure":        (20.0,   200.0),
        "motor_temp_f":               (150.0,  250.0),
        "motor_current_amps":         (10.0,   200.0),
        "freq_hz":                    (0.0,    100.0),
        "pump_intake_pressure_psi":   (0.0,    5000.0),
        "pump_discharge_pressure_psi":(0.0,    5000.0),
        "vibration_hz":               (0.0,    1.25),
    },
    "Gas Lift": {
        "allocated_oil":                 (1.0,    2500.0),
        "test_oil":                      (0.0,    0.0),
        "avg_casing_pressure":           (50.0,   5000.0),
        "casing_injection_pressure_psi": (800.0,  1200.0),
        "tubing_pressure_psi":           (50.0,   150.0),
        "injection_rate_mscfd":          (0.0,    50000.0),
        "wellhead_temp_f":               (10.0,   400.0),
        "choke_position_pct":            (0.0,    100.0),
        "motor_temp_f":                  (80.0,   130.0),
    },
}

SENSOR_FAILURE_DIRECTION = {
    "motor_current_amps": "up", "pump_fillage_pct": "down", "strokes_per_min": "down",
    "gross_stroke_len": "down", "net_stroke_len": "down", "motor_temp_f": "up",
    "avg_tubing_pressure": "down", "avg_casing_pressure": "up", "freq_hz": "down",
    "pump_intake_pressure": "down", "pump_intake_pressure_psi": "down",
    "pump_discharge_pressure_psi": "down", "vibration_hz": "up",
    "casing_injection_pressure_psi": "down", "injection_rate_mscfd": "down",
    "wellhead_temp_f": "down", "choke_position_pct": "down",
    "casing_pressure_psi": "up", "tubing_pressure_psi": "down",
}

SENSOR_FAILURE_TARGET = {
    "motor_current_amps": 180.0, "pump_fillage_pct": 15.0, "strokes_per_min": 1.0,
    "gross_stroke_len": 10.0, "net_stroke_len": 5.0, "motor_temp_f": 320.0,
    "avg_tubing_pressure": 50.0, "avg_casing_pressure": 4000.0, "freq_hz": 45.0,
    "pump_intake_pressure": 50.0, "pump_intake_pressure_psi": 200.0,
    "pump_discharge_pressure_psi": 500.0, "vibration_hz": 1.1,
    "casing_injection_pressure_psi": 300.0, "injection_rate_mscfd": 500.0,
    "wellhead_temp_f": 15.0, "choke_position_pct": 5.0,
    "casing_pressure_psi": 4500.0, "tubing_pressure_psi": 20.0,
}

def normal_reading(sensor, normals):
    lo, hi = normals[sensor]
    mid    = (lo + hi) / 2
    noise  = (hi - lo) * 0.04
    return round(float(np.clip(np.random.normal(mid, noise), lo, hi)), 2)

def init_sensor_state(sensor, normals):
    lo, hi = normals[sensor]
    mid    = (lo + hi) / 2
    noise  = (hi - lo) * 0.04
    return float(np.clip(np.random.normal(mid, noise), lo, hi))

def step_deteriorating_sensor(current_val, sensor, normals, day, total_window,
                               is_trigger, onset_day):
    lo, hi        = normals[sensor]
    rng           = hi - lo
    direction     = SENSOR_FAILURE_DIRECTION.get(sensor, "up")
    days_active   = max(0, day - onset_day)
    if days_active <= 0:
        return round(float(np.clip(np.random.normal(current_val, rng * 0.025), lo, hi)), 2)
    active_window = max(total_window - onset_day, 1)
    progress      = min(days_active / active_window, 1.0)
    base_drift    = (0.008 if is_trigger else 0.003) * rng
    if progress > 0.85:
        accel = 1.0 + (progress - 0.85) * 12.0
    else:
        accel = 1.0 + progress * 0.5
    drift_scale   = base_drift * accel
    bias = drift_scale * progress if direction == "up" else -drift_scale * progress
    recovery_prob = max(0.0, 0.45 - progress * 0.45)
    if random.random() < recovery_prob:
        bias *= -random.uniform(0.4, 1.0)
    noise   = rng * (0.025 + progress * (0.02 if is_trigger else 0.01))
    new_val = current_val + bias + float(np.random.normal(0, noise))
    new_val = max(new_val, 0.0)
    new_val = min(new_val, hi * 2.0)
    return round(new_val, 2)

def step_gross_net_stroke(gross_cur, net_cur, gross_base, net_base,
                           day, total_window, onset_day):
    days_active = max(0, day - onset_day)
    if days_active <= 0:
        gross = max(1.0, float(np.random.normal(gross_base, gross_base * 0.01)))
        net   = max(1.0, float(min(np.random.normal(net_base, net_base * 0.012), gross * 0.98)))
        return round(gross, 2), round(net, 2)
    active_window = max(total_window - onset_day, 1)
    progress      = min(days_active / active_window, 1.0)
    g_drift = gross_base * 0.002 * (1.0 + progress * 2.5)
    n_drift = net_base   * 0.004 * (1.0 + progress * 3.0)
    g_bias  = -g_drift * progress
    n_bias  = -n_drift * progress
    if random.random() < max(0.0, 0.40 - progress * 0.40):
        g_bias *= -random.uniform(0.3, 0.8)
        n_bias *= -random.uniform(0.3, 0.8)
    gross = max(1.0, gross_cur + g_bias + float(np.random.normal(0, gross_base * 0.008)))
    net   = max(1.0, net_cur   + n_bias + float(np.random.normal(0, net_base   * 0.010)))
    net   = min(net, gross * 0.98)
    return round(gross, 2), round(net, 2)

def simulate_well(asset_id, lift, spud_date, base_rate, gross_base, net_base):
    dates   = []
    current = spud_date
    while current <= END_DATE:
        dates.append(current)
        current += timedelta(days=1)

    days = len(dates)
    t    = np.arange(days)

    variance_map = {"Gas Lift": 0.050, "Pumping Unit": 0.035, "ESP": 0.020}
    b_map  = {"Pumping Unit": random.uniform(0.5, 0.8),
               "ESP":          random.uniform(0.8, 1.2),
               "Gas Lift":     random.uniform(0.7, 1.0)}
    Di_map = {"Pumping Unit": random.uniform(0.0004, 0.0008),
               "ESP":          random.uniform(0.0008, 0.0015),
               "Gas Lift":     random.uniform(0.0006, 0.0012)}
    floor_map = {"Pumping Unit": random.uniform(8.0, 20.0),
                  "ESP":          random.uniform(30.0, 80.0),
                  "Gas Lift":     random.uniform(15.0, 40.0)}

    b     = b_map[lift]
    Di    = Di_map[lift]
    floor = floor_map[lift]
    theoretical  = base_rate / np.power(1.0 + b * Di * t, 1.0 / b)
    theoretical  = np.maximum(theoretical, floor)
    prod_noise   = np.random.normal(1.0, variance_map[lift], days)
    rates        = np.clip(theoretical * prod_noise, floor * 0.5, None)

    PROD_PARAMS = {
        "Pumping Unit": {"water_base":(50.0,800.0),"gas_base":(10.0,150.0),
                         "water_noise":0.06,"gas_noise":0.05,
                         "water_decline":0.0002,"gas_decline":0.0008,"water_rise":0.0001},
        "ESP":          {"water_base":(200.0,5000.0),"gas_base":(50.0,500.0),
                         "water_noise":0.05,"gas_noise":0.04,
                         "water_decline":0.0001,"gas_decline":0.0007,"water_rise":0.00015},
        "Gas Lift":     {"water_base":(100.0,2000.0),"gas_base":(100.0,2000.0),
                         "water_noise":0.07,"gas_noise":0.06,
                         "water_decline":0.00015,"gas_decline":0.0006,"water_rise":0.00012},
    }
    pp           = PROD_PARAMS[lift]
    water_base_v = random.uniform(*pp["water_base"])
    gas_base_v   = random.uniform(*pp["gas_base"])
    water_th     = water_base_v*(1+pp["water_rise"]*t)*np.exp(-pp["water_decline"]*t)
    gas_th       = gas_base_v*np.exp(-pp["gas_decline"]*t)
    water_rates  = np.clip(water_th*np.random.normal(1.0,pp["water_noise"],days), 1.0, None)
    gas_rates    = np.clip(gas_th  *np.random.normal(1.0,pp["gas_noise"],  days), 0.5, None)

    test_day_of_month = random.randint(5, 25)
    normals     = {k: v for k, v in SENSOR_NORMALS[lift].items()
                   if k not in ('allocated_oil', 'test_oil', 'gross_stroke_len', 'net_stroke_len')}
    sensor_keys = list(normals.keys())

    history       = []
    status        = "PRODUCING"
    days_online   = 0
    downtime_rem  = 0
    failure_cause = ""
    detr_active      = False
    detr_total_days  = 0
    detr_elapsed     = 0
    detr_failure_def = None
    sensor_state     = {}
    sensor_onset     = {}
    gross_cur        = gross_base
    net_cur          = net_base

    for i, current_date in enumerate(dates):
        date_str    = current_date.strftime("%Y-%m-%d")
        is_test_day = (current_date.day == test_day_of_month)
        row         = {"date": date_str}

        if status == "PRODUCING":
            days_online += 1
            fail_prob = 0.00005
            if lift == "ESP" and days_online > 450:
                fail_prob += 0.003 * ((days_online - 450) / 100)
            elif lift == "Pumping Unit" and days_online > 300:
                fail_prob += 0.002 * ((days_online - 300) / 100)
            elif lift == "Gas Lift" and days_online > 600:
                fail_prob += 0.001 * ((days_online - 600) / 150)

            if not detr_active and random.random() < fail_prob:
                detr_active      = True
                detr_failure_def = random.choice(FAILURES[lift])
                detr_total_days  = random.randint(detr_failure_def["window_min"],
                                                  detr_failure_def["window_max"])
                detr_elapsed     = 0
                trigger_sens     = detr_failure_def["trigger_sensor"]
                sensor_state = {s: init_sensor_state(s, normals) for s in sensor_keys}
                sensor_onset = {}
                for s in sensor_keys:
                    if s == trigger_sens:
                        sensor_onset[s] = 0
                    else:
                        sensor_onset[s] = random.randint(int(detr_total_days * 0.10),
                                                         int(detr_total_days * 0.50))
                gross_cur = gross_base
                net_cur   = net_base

            if detr_active:
                detr_elapsed += 1
                trigger_sens  = detr_failure_def["trigger_sensor"]
                new_sensor_vals = {}
                for s in sensor_keys:
                    new_val = step_deteriorating_sensor(
                        sensor_state[s], s, normals,
                        detr_elapsed, detr_total_days,
                        is_trigger=(s == trigger_sens),
                        onset_day=sensor_onset[s])
                    sensor_state[s]    = new_val
                    new_sensor_vals[s] = new_val
                if lift == "Pumping Unit":
                    gross_cur, net_cur = step_gross_net_stroke(
                        gross_cur, net_cur, gross_base, net_base,
                        detr_elapsed, detr_total_days,
                        onset_day=sensor_onset.get("gross_stroke_len", 0))
                if detr_elapsed >= detr_total_days:
                    status        = "SHUT-IN"
                    failure_cause = detr_failure_def["cause"]
                    downtime_rem  = random.randint(4, 12)
                    detr_active   = False
                    detr_failure_def = None
                    sensor_state  = {}
                    sensor_onset  = {}
                    row.update({s: 0.0 for s in sensor_keys})
                    row.update({"allocated_oil": 0.0, "test_oil": None,
                                "allocated_water": 0.0, "allocated_gas": 0.0,
                                "run_status": "SHUT-IN", "failure_cause": failure_cause})
                    if lift == "Pumping Unit":
                        row["gross_stroke_len"] = 0.0
                        row["net_stroke_len"]   = 0.0
                    history.append(row)
                    continue
                sensor_vals = new_sensor_vals
            else:
                sensor_vals = {s: normal_reading(s, normals) for s in sensor_keys}
                if lift == "Pumping Unit":
                    gross_cur = max(1.0, float(np.random.normal(gross_base, gross_base * 0.01)))
                    net_cur   = max(1.0, float(min(np.random.normal(net_base, net_base * 0.015),
                                                    gross_cur * 0.98)))

            oil = round(float(rates[i]), 2)
            row["allocated_oil"] = oil
            row["test_oil"] = round(oil * (1 + random.choice([1,-1]) * random.uniform(0.05,0.12)), 2) \
                              if is_test_day else None
            if lift == "Pumping Unit":
                row["gross_stroke_len"] = round(gross_cur, 2)
                row["net_stroke_len"]   = round(net_cur, 2)
            for s, v in sensor_vals.items():
                row[s] = v
            row.update({"run_status": "PRODUCING", "failure_cause": "",
                        "allocated_water": round(float(water_rates[i]), 2),
                        "allocated_gas":   round(float(gas_rates[i]),   2)})
        else:
            row.update({s: 0.0 for s in sensor_keys})
            row.update({"allocated_oil": 0.0, "test_oil": None,
                        "allocated_water": 0.0, "allocated_gas": 0.0,
                        "run_status": "SHUT-IN", "failure_cause": failure_cause})
            if lift == "Pumping Unit":
                row["gross_stroke_len"] = 0.0
                row["net_stroke_len"]   = 0.0
            downtime_rem -= 1
            if downtime_rem <= 0:
                status        = "PRODUCING"
                days_online   = 0
                failure_cause = ""
                gross_cur     = gross_base
                net_cur       = net_base
        history.append(row)
    return history

def build_wells():
    wells    = []
    lift_idx = 0
    for lease in LEASES:
        for w in range(1, WELLS_PER_LEASE + 1):
            well_num   = LEASES.index(lease) * WELLS_PER_LEASE + w
            lift       = LIFT_POOL[lift_idx % len(LIFT_POOL)]
            lift_idx  += 1
            suffix     = "V" if lift == "Pumping Unit" else "H"
            asset_id   = f"{lease}-12-{well_num}-{suffix}"
            gross_base = random.uniform(120.0, 168.0)
            net_base   = gross_base * random.uniform(0.85, 0.92)
            wells.append({
                "asset_id":   asset_id,
                "lease":      lease,
                "lift":       lift,
                "spud_date":  random_spud(),
                "base_rate":  random.uniform(300, 1200),
                "gross_base": gross_base,
                "net_base":   net_base,
            })
    return wells

def get_personnel(lease):
    return {
        "prod_technician": {
            "name":  f"{lease} Production Tech",
            "role":  "Production Technician",
            "email": f"prodtech.{lease.lower()}@redbluffresources.com"
        },
        "well_perf_engineer": {
            "name":  random.choice(["Derek Tatum", "Kristen Albright"]),
            "role":  "Well Performance Engineer",
            "email": "wellperf@redbluffresources.com"
        }
    }

# ── DB helpers ────────────────────────────────────────────────────────────────
CHUNK = 5000

def _bulk_insert(cursor, sql, rows):
    for i in range(0, len(rows), CHUNK):
        execute_values(cursor, sql, rows[i:i+CHUNK])

def write_to_db(assets_map):
    conn = psycopg2.connect(
        dbname="oilfield",
        user="postgres",
        password="postgres",
        host="127.0.0.1",
        port="5433",
    )
    cursor = conn.cursor()
    # ── rb_assets ─────────────────────────────────────────────────────────────
    print("  Inserting rb_assets...")
    asset_rows = [(aid, a["type"], a["lift"]) for aid, a in assets_map.items()]
    _bulk_insert(cursor, """
        INSERT INTO rb_assets (asset_id, type, lift)
        VALUES %s
        ON CONFLICT (asset_id) DO UPDATE SET type=EXCLUDED.type, lift=EXCLUDED.lift
    """, asset_rows)

    # ── rb_personnel ──────────────────────────────────────────────────────────
    print("  Inserting rb_personnel...")
    pers_rows = []
    for aid, asset in assets_map.items():
        for role_key, person in asset["assigned_personnel"].items():
            pers_rows.append((aid, role_key, person["name"], person["role"], person["email"]))
    _bulk_insert(cursor, """
        INSERT INTO rb_personnel (asset_id, role_key, name, role, email)
        VALUES %s
    """, pers_rows)

    # ── rb_well_history ───────────────────────────────────────────────────────
    print("  Inserting rb_well_history...")
    hist_rows = []
    for aid, asset in assets_map.items():
        lift = asset["lift"]
        for day in asset["history"]:
            hist_rows.append((
                aid, day["date"],
                day.get("allocated_oil"),  day.get("test_oil"),
                day.get("allocated_water"), day.get("allocated_gas"),
                # Pumping Unit
                day.get("gross_stroke_len"), day.get("net_stroke_len"),
                day.get("strokes_per_min"),  day.get("pump_fillage_pct"),
                day.get("casing_pressure_psi"), day.get("tubing_pressure_psi"),
                day.get("motor_current_amps"),  day.get("freq_hz"),
                day.get("pump_intake_pressure"), day.get("motor_temp_f"),
                # ESP
                day.get("avg_tubing_pressure"),         day.get("avg_casing_pressure"),
                day.get("pump_intake_pressure_psi"),    day.get("pump_discharge_pressure_psi"),
                day.get("vibration_hz"),
                # Gas Lift
                day.get("casing_injection_pressure_psi"), day.get("injection_rate_mscfd"),
                day.get("wellhead_temp_f"),                day.get("choke_position_pct"),
                # Status
                day.get("run_status"), day.get("failure_cause", ""),
            ))

    _bulk_insert(cursor, """
        INSERT INTO rb_well_history (
            asset_id, date,
            allocated_oil, test_oil, allocated_water, allocated_gas,
            gross_stroke_len, net_stroke_len, strokes_per_min, pump_fillage_pct,
            casing_pressure_psi, tubing_pressure_psi, motor_current_amps, freq_hz,
            pump_intake_pressure, motor_temp_f,
            avg_tubing_pressure, avg_casing_pressure,
            pump_intake_pressure_psi, pump_discharge_pressure_psi, vibration_hz,
            casing_injection_pressure_psi, injection_rate_mscfd,
            wellhead_temp_f, choke_position_pct,
            run_status, failure_cause
        ) VALUES %s
    """, hist_rows)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    print(f"Red Bluff Resources — Digital Twin")
    print(f"60 wells | End date: {END_DATE.strftime('%Y-%m-%d')}")
    print("─" * 60)

    wells      = build_wells()
    assets_map = {}

    for i, w in enumerate(wells):
        aid = w["asset_id"]
        print(f"  [{i+1:>2}/60] {aid} ({w['lift']}, spud {w['spud_date'].strftime('%Y-%m-%d')})...")
        history = simulate_well(aid, w["lift"], w["spud_date"],
                                w["base_rate"], w["gross_base"], w["net_base"])
        assets_map[aid] = {
            "type":               "Well",
            "lift":               w["lift"],
            "assigned_personnel": get_personnel(w["lease"]),
            "history":            history,
        }

    total_days     = sum(len(a["history"]) for a in assets_map.values())
    total_failures = sum(sum(1 for d in a["history"] if d["run_status"] == "SHUT-IN")
                         for a in assets_map.values())
    print(f"\n  Assets:           {len(assets_map)}")
    print(f"  Total daily rows: {total_days:,}")
    print(f"  Failure events:   {total_failures:,}")

    print("\nWriting to PostgreSQL...")
    write_to_db(assets_map)
    print("✓ Done — rb_assets, rb_personnel, rb_well_history populated")