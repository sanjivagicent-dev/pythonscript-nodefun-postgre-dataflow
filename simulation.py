"""
Apex Permian — Engineering-Grade Digital Twin v4.0
===================================================
Implements all 7 improvements from the Predictive Maintenance Data Realism Brief:

  P1  Pre-failure degradation ramps (convex, lift-type-specific)
  P2  Mechanistically distinct failure modes with inter-sensor causal lags
  P3  AR(1) correlated noise (autocorrelated sensor readings)
  P4  Non-failure operational events (choke adjust, chemical treatment, etc.)
  P5  Realistic post-failure recovery ramps with permanent damage offset
  P6  Early / Mid / Late failure differentiation (severity + lead time)
  P7  New schema fields: degradation_phase, failure_mode, days_to_failure,
      is_operational_event, event_type, post_failure_recovery_day,
      noise_seed_ar1 (internal), pre_failure_ramp_intensity

New DB/Parquet columns added (listed at bottom of file).
"""

import json
import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# ── DB connection ────────────────────────────────────────────────────────────
conn = psycopg2.connect(
    dbname="oilfield",
    user="postgres",
    password="postgres",
    host="127.0.0.1",
    port="5433",
)
cursor = conn.cursor()

# ── CONFIGURATION ────────────────────────────────────────────────────────────
START_DATE = datetime(2014, 1, 1)
END_DATE   = datetime(2024, 6, 1)
TOTAL_DAYS = (END_DATE - START_DATE).days

# ── FAILURE LIBRARY ──────────────────────────────────────────────────────────
# Each entry: (label, timing_class, lead_weeks_range, downtime_range)
FAILURE_MODES = {
    "Rod Pump": {
        "Early": [
            ("Polished Rod Clamp Slip", "Early", (4, 6), (2,  5)),
            ("Misaligned Unit",         "Early", (4, 6), (2,  5)),
        ],
        "Mid": [
            ("Parted Rods",    "Mid",  (2, 3), (5, 10)),
            ("Gas Lock",       "Mid",  (2, 3), (4,  8)),
            ("Stuck Pump",     "Mid",  (2, 3), (5, 10)),
            ("Gearbox Failure","Mid",  (2, 3), (4,  8)),
        ],
        "Late": [
            ("Hole in Tubing",          "Late", (1, 2), (7, 14)),
            ("Casing Leak",             "Late", (1, 2), (7, 14)),
            ("Unit Structural Fatigue", "Late", (1, 2), (5, 12)),
        ],
    },
    "ESP": {
        "Early": [
            ("Splicing Failure",  "Early", (4, 6), (3,  7)),
            ("Improper Cooling",  "Early", (4, 6), (3,  7)),
        ],
        "Mid": [
            ("Scale Deposition", "Mid",  (3, 5), (5, 10)),
            ("Motor Overheating","Mid",  (2, 4), (4,  8)),
            ("Broken Shaft",     "Mid",  (2, 3), (5, 10)),
            ("Cable Short",      "Mid",  (2, 3), (4,  8)),
            ("Vibration Trip",   "Mid",  (2, 3), (3,  7)),
        ],
        "Late": [
            ("Pump Stage Wear",    "Late", (1, 2), (7, 14)),
            ("Severe Scale Depo.", "Late", (1, 2), (7, 14)),
        ],
    },
    "Gas Lift": {
        "Early": [
            ("Valve Installation Error", "Early", (4, 6), (3, 6)),
        ],
        "Mid": [
            ("Compressor Trip",    "Mid",  (2, 3), (4,  8)),
            ("Injection Line Freeze", "Mid", (2, 3), (3,  7)),
            ("Liquid Loading",     "Mid",  (3, 5), (5, 10)),
        ],
        "Late": [
            ("Scale Restriction", "Late", (1, 2), (7, 14)),
            ("Severe Liquid Load","Late", (1, 2), (5, 12)),
        ],
    },
}

# ── OPERATIONAL EVENTS (non-failure) ────────────────────────────────────────
OPERATIONAL_EVENTS = {
    "Rod Pump": [
        {
            "name": "Choke Adjustment",
            "duration": (1, 3),
            "sensor_delta": {"avg_tubing_pressure": -0.15, "true_oil": 0.10},
        },
        {
            "name": "Chemical Treatment",
            "duration": (1, 2),
            "sensor_delta": {"avg_tubing_pressure": 0.05, "true_oil": -0.05},
        },
        {
            "name": "Pump Speed Change",
            "duration": (1, 2),
            "sensor_delta": {"spm": 0.15, "true_oil": 0.08, "avg_motor_amps": 0.10},
        },
        {
            "name": "Planned Workover",
            "duration": (3, 5),
            "sensor_delta": {"true_oil": -1.0},  # shut-in during workover
        },
    ],
    "ESP": [
        {
            "name": "Frequency Ramp",
            "duration": (1, 2),
            "sensor_delta": {"freq_hz": 0.10, "avg_motor_amps": 0.08, "true_oil": 0.07},
        },
        {
            "name": "Chemical Injection",
            "duration": (1, 3),
            "sensor_delta": {"pump_intake_pressure": 0.05},
        },
        {
            "name": "Planned Shutdown",
            "duration": (2, 4),
            "sensor_delta": {"true_oil": -1.0},
        },
    ],
    "Gas Lift": [
        {
            "name": "Injection Rate Adjustment",
            "duration": (1, 2),
            "sensor_delta": {"injection_rate_mcf": 0.12, "true_oil": 0.06},
        },
        {
            "name": "Valve Change",
            "duration": (2, 4),
            "sensor_delta": {"avg_casing_pressure": 0.10, "true_oil": -0.05},
        },
        {
            "name": "Chemical Treatment",
            "duration": (1, 3),
            "sensor_delta": {"avg_tubing_pressure": -0.08},
        },
    ],
}


# ── AR(1) NOISE ──────────────────────────────────────────────────────────────
def ar1_noise(n: int, sigma: float = 1.0, phi: float = 0.7) -> np.ndarray:
    """Autoregressive order-1 noise.  phi=0 → white noise; phi→1 → random walk."""
    eps = np.random.normal(0, sigma * np.sqrt(1 - phi**2), n)
    out = np.zeros(n)
    out[0] = eps[0]
    for i in range(1, n):
        out[i] = phi * out[i - 1] + eps[i]
    return out


# ── CONVEX DEGRADATION RAMP ──────────────────────────────────────────────────
def convex_ramp(length: int, max_degradation: float) -> np.ndarray:
    """Returns a convex decay multiplier array: starts near 1.0, steepens toward
    (1 - max_degradation) at the end.  Shape: x^2 curve (convex, accelerating)."""
    t = np.linspace(0, 1, length)
    return 1.0 - max_degradation * (t ** 2)


# ── ASSET BUILDER ────────────────────────────────────────────────────────────
def build_hierarchy():
    well_registry = []
    facility_registry = {}

    lifts   = ["Rod Pump", "ESP", "Gas Lift"]
    orients = ["Horizontal", "Vertical"]

    definitions = []
    for lift in lifts:
        for orient in orients:
            for _ in range(5):
                definitions.append({"lift": lift, "orient": orient})
    random.shuffle(definitions)

    status_pool = ["Happy Path"] * 12 + ["Failure"] * 9 + ["Degrading"] * 9
    random.shuffle(status_pool)

    leases = ["Mustang", "Bronco", "Ranger", "Pinto"]

    for i in range(30):
        defn  = definitions[i]
        lease = leases[i % len(leases)]
        sect  = [12, 22, 14, 4][i % 4]

        fac_id    = f"{lease}-{defn['orient']}-Battery"
        age_days  = random.randint(365, TOTAL_DAYS - 30)
        spud_date = END_DATE - timedelta(days=age_days)

        if fac_id not in facility_registry:
            facility_registry[fac_id] = spud_date
        elif spud_date < facility_registry[fac_id]:
            facility_registry[fac_id] = spud_date

        exist  = len([w for w in well_registry if w["lease"] == lease]) + 1
        w_name = f"{lease}-{sect}-{exist}-{defn['orient'][0]}"

        well_registry.append(
            {
                "asset_id":            w_name,
                "lease":               lease,
                "facility_id":         fac_id,
                "lift_type":           defn["lift"],
                "orientation":         defn["orient"],
                "spud_date":           spud_date,
                "current_status_goal": status_pool[i],
                "base_rate":           800 if defn["orient"] == "Horizontal" else 150,
            }
        )

    return well_registry, facility_registry


# ── PHYSICS ENGINE ────────────────────────────────────────────────────────────
def generate_well_timeline(well: dict) -> pd.DataFrame:
    dates = pd.date_range(well["spud_date"], END_DATE, freq="D")
    days  = len(dates)
    if days < 1:
        return pd.DataFrame()

    lift = well["lift_type"]
    rng  = np.random.default_rng()  # local rng for vectorised ops

    # ── A. Base Production (exponential decline) + AR(1) noise ───────────────
    t       = np.arange(days)
    decline = 0.0006 if well["orientation"] == "Horizontal" else 0.0002
    rates   = well["base_rate"] * np.exp(-decline * t)
    rates  += ar1_noise(days, sigma=well["base_rate"] * 0.03, phi=0.6)
    rates   = np.clip(rates, 0, None)

    # ── B. Lift-specific sensor baselines ────────────────────────────────────
    tp  = (rates * 0.1)  + 200 + ar1_noise(days, sigma=3.0,  phi=0.65)
    cp  = (rates * 0.05) + 50  + ar1_noise(days, sigma=2.0,  phi=0.65)

    gross_stroke = np.full(days, np.nan)
    net_stroke   = np.full(days, np.nan)
    spm          = np.full(days, np.nan)
    fillage      = np.full(days, np.nan)
    freq_hz      = np.full(days, np.nan)
    motor_amps   = np.full(days, np.nan)
    pip          = np.full(days, np.nan)
    motor_temp   = np.full(days, np.nan)
    inj_rate     = np.full(days, np.nan)

    if lift == "Rod Pump":
        gross_base   = 144 if well["orientation"] == "Horizontal" else 100
        gross_stroke = np.full(days, gross_base, dtype=float) + ar1_noise(days, 0.5, 0.5)
        net_stroke   = gross_stroke * (np.random.uniform(0.85, 0.90, days) + ar1_noise(days, 0.005, 0.4))
        spm          = 8.5  + ar1_noise(days, sigma=0.2,  phi=0.6)
        fillage      = np.clip(92 + ar1_noise(days, sigma=2.0, phi=0.7), 0, 100)
        motor_amps   = (rates / 10) + 20 + ar1_noise(days, sigma=1.5, phi=0.6)
        pip          = 300 + (rates * 0.3) + ar1_noise(days, sigma=12, phi=0.65)

    elif lift == "ESP":
        freq_hz    = np.clip(58 + ar1_noise(days, sigma=0.8, phi=0.6), 0, 65)
        motor_amps = (rates / 8) + 30 + ar1_noise(days, sigma=2.5, phi=0.65)
        pip        = 500 + (rates * 0.2) + ar1_noise(days, sigma=8, phi=0.65)
        motor_temp = 170 + (motor_amps * 0.5) + ar1_noise(days, sigma=1.5, phi=0.7)

    elif lift == "Gas Lift":
        inj_rate = 600 + ar1_noise(days, sigma=15, phi=0.6)
        inj_press = 900 + ar1_noise(days, sigma=12, phi=0.6)
        cp        = inj_press.copy()
        tp        = inj_press * 0.3 + ar1_noise(days, sigma=3, phi=0.5)

    # ── New annotation columns ────────────────────────────────────────────────
    degradation_phase       = np.full(days, "Normal",  dtype=object)
    failure_mode_col        = np.full(days, "",        dtype=object)
    days_to_failure_col     = np.full(days, -1,        dtype=int)
    is_op_event             = np.zeros(days, dtype=bool)
    event_type_col          = np.full(days, "",        dtype=object)
    post_recovery_day_col   = np.full(days, -1,        dtype=int)
    ramp_intensity_col      = np.zeros(days, dtype=float)

    # ── C. FAILURE EVENTS ────────────────────────────────────────────────────
    num_failures = max(1, int(days / random.randint(180, 450)))
    fail_indices = []
    if days > 120:
        fail_indices = sorted(random.sample(range(60, days - 60), k=min(num_failures, days // 180)))

    status_col = np.full(days, "OK", dtype=object)
    notes_col  = np.full(days, "",  dtype=object)

    for fail_idx in fail_indices:
        # ── Pick failure mode ────────────────────────────────────────────────
        timing_class = random.choice(["Early", "Mid", "Late"])
        mode_list    = FAILURE_MODES[lift].get(timing_class, FAILURE_MODES[lift]["Mid"])
        mode_entry   = random.choice(mode_list)
        mode_name, mode_class, lead_range, down_range = mode_entry

        lead_weeks = random.randint(*lead_range)
        lead_days  = lead_weeks * 7
        downtime   = random.randint(*down_range)

        ramp_start = max(0, fail_idx - lead_days)
        ramp_len   = fail_idx - ramp_start
        event_end  = min(fail_idx + downtime, days)

        # Degradation depth depends on failure timing class
        depth_map  = {"Early": 0.20, "Mid": 0.45, "Late": 0.75}
        max_depth  = depth_map[mode_class]
        ramp       = convex_ramp(ramp_len, max_depth) if ramp_len > 0 else np.array([])

        # ── Annotate days-to-failure in the ramp window ──────────────────────
        for offset in range(ramp_len):
            day_i = ramp_start + offset
            remaining = fail_idx - day_i
            if days_to_failure_col[day_i] < 0 or remaining < days_to_failure_col[day_i]:
                days_to_failure_col[day_i] = remaining
            degradation_phase[day_i]   = "Pre-Failure"
            failure_mode_col[day_i]    = mode_name
            ramp_intensity_col[day_i]  = 1.0 - ramp[offset]  # 0→1 as failure nears

        # ── Apply mechanistically-distinct pre-failure sensor ramps ──────────
        if ramp_len > 0:
            _apply_prefailure_ramp(
                mode_name, lift, ramp_start, ramp_len, ramp, fail_idx,
                rates, fillage, motor_amps, pip, motor_temp, inj_rate, tp, cp, spm,
                net_stroke, freq_hz,
            )

        # ── Failure window: clamp to failure state ────────────────────────────
        rates[fail_idx:event_end]      = 0
        status_col[fail_idx:event_end] = "FAILURE"
        degradation_phase[fail_idx:event_end] = "Failure"
        failure_mode_col[fail_idx:event_end]  = mode_name
        days_to_failure_col[fail_idx]         = 0

        _apply_failure_clamp(
            mode_name, lift, fail_idx, event_end,
            motor_amps, fillage, pip, motor_temp, inj_rate, cp, spm, net_stroke, freq_hz,
        )

        notes_col[fail_idx] = f"ALERT: {mode_name}."
        if event_end < days:
            notes_col[event_end] = "Workover Complete."

        # ── Post-failure recovery ramp ────────────────────────────────────────
        recovery_len = random.randint(3, 8)
        damage_pct   = random.uniform(0.05, 0.20)  # permanent damage fraction
        recovery_end = min(event_end + recovery_len, days)

        for rec_i in range(event_end, recovery_end):
            offset_in_recovery = rec_i - event_end
            frac = (offset_in_recovery + 1) / recovery_len
            pre_fail_rate = rates[max(0, ramp_start - 1)] if ramp_start > 0 else well["base_rate"]
            new_baseline  = pre_fail_rate * (1.0 - damage_pct)
            rates[rec_i]  = new_baseline * frac + ar1_noise(1, sigma=new_baseline * 0.03, phi=0.0)[0]
            status_col[rec_i]              = "RECOVERING"
            degradation_phase[rec_i]       = "Post-Failure"
            post_recovery_day_col[rec_i]   = offset_in_recovery + 1
            failure_mode_col[rec_i]        = mode_name

    # ── D. END-OF-LIFE degradation for "Degrading" wells ─────────────────────
    last_30 = slice(-30, None)
    if well["current_status_goal"] == "Degrading":
        decay = np.linspace(1.0, 0.7, 30)
        rates[last_30] *= decay
        degradation_phase[-30:] = np.where(
            degradation_phase[-30:] == "Normal", "Pre-Failure", degradation_phase[-30:]
        )
        if lift == "ESP":
            pip[last_30]        *= decay
            motor_temp[last_30] *= 1.1
        elif lift == "Rod Pump":
            fillage[last_30] = np.clip(fillage[last_30] * decay, 0, 100)
            pip[last_30]     *= decay

    # ── E. NON-FAILURE OPERATIONAL EVENTS ─────────────────────────────────────
    op_events = OPERATIONAL_EVENTS.get(lift, [])
    n_ops     = max(1, days // 90)
    if days > 60:
        op_indices = random.sample(range(10, days - 10), k=min(n_ops, len(range(10, days - 10))))
        for op_idx in op_indices:
            # Skip if already in a failure or recovery window
            if status_col[op_idx] != "OK":
                continue
            ev       = random.choice(op_events)
            duration = random.randint(*ev["duration"])
            ev_end   = min(op_idx + duration, days)

            for d in range(op_idx, ev_end):
                is_op_event[d]      = True
                event_type_col[d]   = ev["name"]
                status_col[d]       = "OP_EVENT"
                notes_col[d]        = f"Operational: {ev['name']}"

            # Apply sensor deltas
            for sensor_key, delta_frac in ev["sensor_delta"].items():
                arr = _sensor_array_by_name(
                    sensor_key, rates, fillage, motor_amps, pip,
                    motor_temp, inj_rate, tp, cp, spm, net_stroke, freq_hz
                )
                if arr is not None:
                    if delta_frac == -1.0:                     # shut-in
                        arr[op_idx:ev_end] = 0
                    else:
                        arr[op_idx:ev_end] *= (1.0 + delta_frac)

    # ── F. TEST PRODUCTION ────────────────────────────────────────────────────
    test_oil   = np.full(days, np.nan)
    test_water = np.full(days, np.nan)
    test_gas   = np.full(days, np.nan)
    curr = random.randint(0, 20)
    while curr < days:
        if rates[curr] > 0:
            test_oil[curr]   = round(rates[curr], 2)
            test_water[curr] = round(rates[curr] * 2.5, 2)
            test_gas[curr]   = round(rates[curr] * 1.5, 2)
        curr += random.randint(10, 25)

    return pd.DataFrame(
        {
            "date":                    dates,
            "asset_id":                well["asset_id"],
            "facility_id":             well["facility_id"],
            "true_oil":                rates,
            "true_water":              rates * 2.5,
            "true_gas":                rates * 1.5,
            "test_oil":                test_oil,
            "test_water":              test_water,
            "test_gas":                test_gas,
            "avg_tubing_pressure":     tp,
            "avg_casing_pressure":     cp,
            "avg_motor_amps":          motor_amps,
            "gross_stroke_len":        gross_stroke,
            "net_stroke_len":          net_stroke,
            "spm":                     spm,
            "pump_fillage_pct":        fillage,
            "freq_hz":                 freq_hz,
            "pump_intake_pressure":    pip,
            "motor_temp_f":            motor_temp,
            "injection_rate_mcf":      inj_rate,
            "status":                  status_col,
            "notes":                   notes_col,
            # ── NEW COLUMNS ──────────────────────────────────────────────────
            "degradation_phase":       degradation_phase,
            "failure_mode":            failure_mode_col,
            "days_to_failure":         days_to_failure_col,
            "is_operational_event":    is_op_event,
            "event_type":              event_type_col,
            "post_failure_recovery_day": post_recovery_day_col,
            "pre_failure_ramp_intensity": ramp_intensity_col,
        }
    )


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _apply_prefailure_ramp(
    mode_name, lift, ramp_start, ramp_len, ramp, fail_idx,
    rates, fillage, motor_amps, pip, motor_temp, inj_rate, tp, cp, spm,
    net_stroke, freq_hz,
):
    """Apply mechanistically distinct pre-failure sensor degradation
    according to Section 3.2 of the brief with causal lags (Section 3.3)."""

    idx = slice(ramp_start, ramp_start + ramp_len)

    if lift == "Rod Pump":
        if "Parted" in mode_name:
            # Fillage drops first (full window), amps follow with a 7-day lag
            fillage[idx] = np.where(
                fillage[idx] > 0, fillage[idx] * ramp, fillage[idx]
            )
            lag = min(7, ramp_len // 2)
            lag_idx = slice(ramp_start + lag, ramp_start + ramp_len)
            lag_ramp = convex_ramp(ramp_len - lag, (1 - ramp[-1]))
            motor_amps[lag_idx] *= lag_ramp

        elif "Gas Lock" in mode_name or "Lock" in mode_name:
            # Fillage oscillates with growing amplitude
            t_osc = np.arange(ramp_len)
            amplitude = np.linspace(2, 12, ramp_len)
            fillage[idx] = np.clip(
                fillage[idx] + amplitude * np.sin(2 * np.pi * t_osc / 5), 0, 100
            )

        elif "Stuck" in mode_name:
            motor_amps[idx] *= np.linspace(1.0, 1.5, ramp_len)
            spm[idx]         = np.clip(spm[idx] * ramp, 0, None)

        elif "Gearbox" in mode_name:
            motor_amps[idx] *= np.linspace(1.0, 1.6, ramp_len)

        elif "Tubing" in mode_name or "Casing" in mode_name:
            rates[idx] *= ramp
            tp[idx]    *= ramp

        else:  # generic rod pump
            fillage[idx]    = np.clip(fillage[idx] * ramp, 0, 100)
            motor_amps[idx] *= ramp

    elif lift == "ESP":
        if "Scale" in mode_name:
            # Amps rise first (full window), PIP drops 4 days later, oil drops 7 days later
            motor_amps[idx] *= np.linspace(1.0, 1.4, ramp_len)
            lag4 = min(4, ramp_len // 2)
            lag7 = min(7, ramp_len * 2 // 3)
            lag4_idx  = slice(ramp_start + lag4, ramp_start + ramp_len)
            lag7_idx  = slice(ramp_start + lag7, ramp_start + ramp_len)
            lag4_ramp = convex_ramp(ramp_len - lag4, 0.30)
            lag7_ramp = convex_ramp(ramp_len - lag7, 0.25)
            if len(lag4_ramp):
                pip[lag4_idx]   *= lag4_ramp
            if len(lag7_ramp):
                rates[lag7_idx] *= lag7_ramp

        elif "Overheat" in mode_name or "Thermal" in mode_name:
            # Temp leads, amps slightly elevated, pip neutral
            motor_temp[idx] *= np.linspace(1.0, 1.12, ramp_len)
            motor_amps[idx] *= np.linspace(1.0, 1.08, ramp_len)

        elif "Shaft" in mode_name:
            motor_amps[idx] *= ramp
            pip[idx]        *= np.linspace(1.0, 1.5, ramp_len)

        elif "Short" in mode_name or "Cable" in mode_name:
            motor_amps[idx] *= np.linspace(1.0, 0.6, ramp_len)
            freq_hz[idx]    *= np.linspace(1.0, 0.85, ramp_len)

        elif "Vibration" in mode_name:
            t_v    = np.arange(ramp_len)
            amp    = np.linspace(1, 4, ramp_len)
            motor_amps[idx] += amp * np.sin(2 * np.pi * t_v / 3)

        else:  # generic ESP
            motor_amps[idx] *= np.linspace(1.0, 1.3, ramp_len)
            pip[idx]        *= ramp

    elif lift == "Gas Lift":
        if "Loading" in mode_name or "Liquid" in mode_name:
            # Injection stable, tubing pressure rises, oil drops — divergence is the signal
            tp[idx]    *= np.linspace(1.0, 1.3, ramp_len)
            rates[idx] *= ramp

        elif "Compressor" in mode_name or "Trip" in mode_name:
            inj_rate[idx] *= np.linspace(1.0, 0.7, ramp_len)
            cp[idx]       *= np.linspace(1.0, 0.7, ramp_len)

        elif "Freeze" in mode_name:
            inj_rate[idx] *= ramp
            cp[idx]       *= ramp

        elif "Scale" in mode_name or "Restriction" in mode_name:
            inj_rate[idx] *= np.linspace(1.0, 0.85, ramp_len)
            tp[idx]       *= np.linspace(1.0, 1.15, ramp_len)
            rates[idx]    *= ramp

        else:  # generic gas lift
            inj_rate[idx] *= ramp
            rates[idx]    *= ramp


def _apply_failure_clamp(
    mode_name, lift, fail_idx, event_end,
    motor_amps, fillage, pip, motor_temp, inj_rate, cp, spm, net_stroke, freq_hz,
):
    """Hard-clamp sensors to failed state during the downtime window."""
    s = slice(fail_idx, event_end)

    if lift == "Rod Pump":
        if "Parted" in mode_name:
            motor_amps[s] *= 0.4
            fillage[s]     = 0
            net_stroke[s]  = 0
            pip[s]        += 200
        elif "Gas Lock" in mode_name or "Lock" in mode_name:
            fillage[s]    = 0
            pip[s]       *= 1.3
        elif "Stuck" in mode_name:
            motor_amps[s] *= 2.0
            spm[s]         = 0
        elif "Gearbox" in mode_name:
            motor_amps[s]  = 0
            spm[s]         = 0
        else:
            motor_amps[s] *= 0.5
            fillage[s]     = 0

    elif lift == "ESP":
        if "Shaft" in mode_name or "Stage" in mode_name:
            motor_amps[s] = 20
            pip[s]        = 2500
        elif "Short" in mode_name or "Cable" in mode_name:
            motor_amps[s] = 0
            freq_hz[s]    = 0
        elif "Overheat" in mode_name or "Thermal" in mode_name:
            motor_temp[s] *= 1.25
            motor_amps[s]  = 0
        else:
            motor_amps[s] *= 0.3

    elif lift == "Gas Lift":
        if "Trip" in mode_name or "Compressor" in mode_name:
            inj_rate[s]  = 0
            cp[s]       *= 0.5
        elif "Freeze" in mode_name:
            inj_rate[s]  = 0
        elif "Loading" in mode_name or "Liquid" in mode_name:
            inj_rate[s] *= 0.6
        else:
            inj_rate[s] *= 0.4


def _sensor_array_by_name(key, rates, fillage, motor_amps, pip,
                           motor_temp, inj_rate, tp, cp, spm, net_stroke, freq_hz):
    """Map a sensor name string to its mutable numpy array."""
    return {
        "true_oil":            rates,
        "avg_tubing_pressure": tp,
        "avg_casing_pressure": cp,
        "avg_motor_amps":      motor_amps,
        "pump_fillage_pct":    fillage,
        "freq_hz":             freq_hz,
        "pump_intake_pressure": pip,
        "motor_temp_f":        motor_temp,
        "injection_rate_mcf":  inj_rate,
        "spm":                 spm,
        "net_stroke_len":      net_stroke,
    }.get(key)


# ── MASTER SIMULATION ─────────────────────────────────────────────────────────
def run_simulation():
    print("Generating Engineering-Grade Digital Twin v4.0...")

    well_meta, fac_meta = build_hierarchy()

    all_dfs = []
    for w in well_meta:
        df = generate_well_timeline(w)
        if not df.empty:
            all_dfs.append(df)

    master_df = pd.concat(all_dfs).sort_values(["date", "facility_id"])
    print(f"Generated rows: {len(master_df):,}")

    # ── INSERT INTO POSTGRESQL ────────────────────────────────────────────────
    print("Bulk inserting into PostgreSQL...")

    insert_query = """
    INSERT INTO well_data (
        date, asset_id, facility_id,
        true_oil, true_water, true_gas,
        test_oil, test_water, test_gas,
        avg_tubing_pressure, avg_casing_pressure, avg_motor_amps,
        gross_stroke_len, net_stroke_len, spm, pump_fillage_pct,
        freq_hz, pump_intake_pressure, motor_temp_f, injection_rate_mcf,
        status, notes,
        degradation_phase, failure_mode, days_to_failure,
        is_operational_event, event_type,
        post_failure_recovery_day, pre_failure_ramp_intensity
    ) VALUES %s
    ON CONFLICT DO NOTHING
    """

    def flt(v):
        return None if pd.isna(v) else float(v)

    def itr(v):
        return None if pd.isna(v) or v == -1 else int(v)

    data = [
        (
            row["date"],
            row["asset_id"],
            row["facility_id"],
            flt(row["true_oil"]),
            flt(row["true_water"]),
            flt(row["true_gas"]),
            flt(row["test_oil"]),
            flt(row["test_water"]),
            flt(row["test_gas"]),
            flt(row["avg_tubing_pressure"]),
            flt(row["avg_casing_pressure"]),
            flt(row["avg_motor_amps"]),
            flt(row["gross_stroke_len"]),
            flt(row["net_stroke_len"]),
            flt(row["spm"]),
            flt(row["pump_fillage_pct"]),
            flt(row["freq_hz"]),
            flt(row["pump_intake_pressure"]),
            flt(row["motor_temp_f"]),
            flt(row["injection_rate_mcf"]),
            row["status"],
            row["notes"],
            # new columns
            row["degradation_phase"],
            row["failure_mode"],
            itr(row["days_to_failure"]),
            bool(row["is_operational_event"]),
            row["event_type"],
            itr(row["post_failure_recovery_day"]),
            flt(row["pre_failure_ramp_intensity"]),
        )
        for _, row in master_df.iterrows()
    ]

    execute_values(cursor, insert_query, data)
    conn.commit()
    print("Data inserted successfully!")

    # ── SAVE PARQUET ──────────────────────────────────────────────────────────
    # master_df.to_parquet("apex_permian_v4.parquet", index=False, engine="pyarrow")
    # print("Parquet saved: apex_permian_v4.parquet")

    # ── BUILD JSON ────────────────────────────────────────────────────────────
    output = {"company": "Apex Permian", "assets": {}}

    for w in well_meta:
        output["assets"][w["asset_id"]] = {
            "type": "Well",
            "lift": w["lift_type"],
            "history": [],
        }
    for fid in fac_meta:
        output["assets"][fid] = {"type": "Facility", "history": []}

    def cj(v):
        return None if pd.isna(v) else round(float(v), 2)

    for fid in master_df["facility_id"].unique():
        fac_df = master_df[master_df["facility_id"] == fid]
        for date, day_data in fac_df.groupby("date"):
            d_str   = date.strftime("%Y-%m-%d")
            esd     = random.random() < 0.0005
            total_oil = day_data["true_oil"].sum() if not esd else 0
            output["assets"][fid]["history"].append(
                {"date": d_str, "oil_prod": round(total_oil, 2), "status": "ESD" if esd else "OK"}
            )
            sum_true = day_data["true_oil"].sum()
            for _, row in day_data.iterrows():
                alloc_oil = 0
                if not esd and sum_true > 0:
                    alloc_oil = total_oil * (row["true_oil"] / sum_true)
                output["assets"][row["asset_id"]]["history"].append(
                    {
                        "date":         d_str,
                        "true_oil":     round(row["true_oil"], 2),
                        "true_water":   round(row["true_water"], 2),
                        "true_gas":     round(row["true_gas"], 2),
                        "allocated_oil": round(alloc_oil, 2),
                        "test_oil":     cj(row["test_oil"]),
                        "test_water":   cj(row["test_water"]),
                        "test_gas":     cj(row["test_gas"]),
                        "avg_tubing_pressure":   cj(row["avg_tubing_pressure"]),
                        "avg_casing_pressure":   cj(row["avg_casing_pressure"]),
                        "avg_motor_amps":         cj(row["avg_motor_amps"]),
                        "gross_stroke_len":       cj(row["gross_stroke_len"]),
                        "net_stroke_len":         cj(row["net_stroke_len"]),
                        "spm":                    cj(row["spm"]),
                        "pump_fillage_pct":       cj(row["pump_fillage_pct"]),
                        "freq_hz":                cj(row["freq_hz"]),
                        "pump_intake_pressure":   cj(row["pump_intake_pressure"]),
                        "motor_temp_f":           cj(row["motor_temp_f"]),
                        "injection_rate_mcf":     cj(row["injection_rate_mcf"]),
                        "status":       "SHUT_IN" if esd else row["status"],
                        "notes":        row["notes"],
                        # new fields
                        "degradation_phase":            row["degradation_phase"],
                        "failure_mode":                 row["failure_mode"],
                        "days_to_failure":              int(row["days_to_failure"]) if row["days_to_failure"] >= 0 else None,
                        "is_operational_event":         bool(row["is_operational_event"]),
                        "event_type":                   row["event_type"],
                        "post_failure_recovery_day":    int(row["post_failure_recovery_day"]) if row["post_failure_recovery_day"] >= 0 else None,
                        "pre_failure_ramp_intensity":   cj(row["pre_failure_ramp_intensity"]),
                    }
                )

    with open("apex_permian_v4.json", "w") as f:
        json.dump(output, f, indent=2, default=str)
    print("JSON saved: apex_permian_v4.json")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    run_simulation()


# =============================================================================
# NEW SCHEMA FIELDS  (add these to your existing well_data table / model)
# =============================================================================
#
# ── PostgreSQL migration (append to your existing CREATE TABLE or run ALTER) ──
#
#   ALTER TABLE well_data
#     ADD COLUMN degradation_phase          TEXT          DEFAULT 'Normal',
#     ADD COLUMN failure_mode               TEXT          DEFAULT '',
#     ADD COLUMN days_to_failure            INTEGER,
#     ADD COLUMN is_operational_event       BOOLEAN       NOT NULL DEFAULT FALSE,
#     ADD COLUMN event_type                 TEXT          DEFAULT '',
#     ADD COLUMN post_failure_recovery_day  INTEGER,
#     ADD COLUMN pre_failure_ramp_intensity DOUBLE PRECISION DEFAULT 0.0;
#
#   -- Recommended indexes for ML feature queries
#   CREATE INDEX idx_wdata_deg_phase   ON well_data (degradation_phase);
#   CREATE INDEX idx_wdata_fail_mode   ON well_data (failure_mode);
#   CREATE INDEX idx_wdata_d2f         ON well_data (days_to_failure);
#   CREATE INDEX idx_wdata_op_event    ON well_data (is_operational_event);
#
#
# ── schema.prisma additions ──────────────────────────────────────────────────
#
#   model WellData {
#     // ... existing fields ...
#
#     degradationPhase          String    @default("Normal")  @map("degradation_phase")
#     failureMode               String    @default("")        @map("failure_mode")
#     daysToFailure             Int?                          @map("days_to_failure")
#     isOperationalEvent        Boolean   @default(false)     @map("is_operational_event")
#     eventType                 String    @default("")        @map("event_type")
#     postFailureRecoveryDay    Int?                          @map("post_failure_recovery_day")
#     preFailureRampIntensity   Float     @default(0.0)       @map("pre_failure_ramp_intensity")
#
#     @@map("well_data")
#   }
#
#
# ── Parquet schema (PyArrow) ─────────────────────────────────────────────────
#
#   import pyarrow as pa
#
#   NEW_FIELDS = [
#       pa.field("degradation_phase",           pa.string()),
#       pa.field("failure_mode",                pa.string()),
#       pa.field("days_to_failure",             pa.int32()),        # null = not in ramp
#       pa.field("is_operational_event",        pa.bool_()),
#       pa.field("event_type",                  pa.string()),
#       pa.field("post_failure_recovery_day",   pa.int32()),        # null = not recovering
#       pa.field("pre_failure_ramp_intensity",  pa.float64()),      # 0.0–1.0
#   ]
#
#   # Append to your existing SCHEMA list and pass to pa.schema([...existing..., *NEW_FIELDS])
#
# =============================================================================