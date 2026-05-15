"""
Red Bluff Resources — Master Builder
========================================
Reads well data from PostgreSQL and adds:
  - Location metadata (basin, sub_basin, state, county, region) to rb_assets
  - Correct personnel assignments to rb_personnel
  - 12 clean wells (3 per lease, mixed lift types) — no failure events in rb_well_history
  - Supporting assets (Facility, Compressor, SWD Well, Tank Battery) per lease
    written to rb_facility_history, rb_compressor_history,
              rb_swd_history, rb_tank_battery_history
  - Supporting asset invoices appended to rb_invoices

Pipeline order:
  1. python3 generate_digital_twin.py   → rb_assets, rb_personnel, rb_well_history
  2. python3 generate_invoices.py       → rb_invoices (wells)
  3. python3 build_master.py            → THIS SCRIPT → location + supporting assets
  4. python3 export_to_parquet.py       → exports all RB tables to .parquet
"""

import numpy as np
import random
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values

# ── DB connection ─────────────────────────────────────────────────────────────
DB_CONFIG = dict(dbname="oilfield", user="postgres", password="postgres",
                 host="127.0.0.1", port="5433")

END_DATE = datetime(2026, 5, 7)
LEASES   = ["Chaparral", "Mesquite", "Caliche", "Llano"]
CHUNK    = 5000

# ── LEASE CONFIG ──────────────────────────────────────────────────────────────
LEASE_ENGINEER = {
    "Chaparral": "Derek Tatum",
    "Mesquite":  "Kristen Albright",
    "Caliche":   "Derek Tatum",
    "Llano":     "Kristen Albright",
}

LEASE_LOCATION = {
    "Chaparral": {"basin":"Permian Basin","sub_basin":"Delaware Basin","state":"TX","county":"Reeves County","region":"West Texas"},
    "Mesquite":  {"basin":"Permian Basin","sub_basin":"Delaware Basin","state":"TX","county":"Loving County","region":"West Texas"},
    "Caliche":   {"basin":"Permian Basin","sub_basin":"Midland Basin","state":"TX","county":"Midland County","region":"West Texas"},
    "Llano":     {"basin":"Permian Basin","sub_basin":"Midland Basin","state":"TX","county":"Ector County","region":"West Texas"},
}

def get_personnel(lease):
    return {
        "prod_technician": {
            "name":  f"{lease} Production Tech",
            "role":  "Production Technician",
            "email": f"prodtech.{lease.lower()}@redbluffresources.com"
        },
        "well_perf_engineer": {
            "name":  LEASE_ENGINEER[lease],
            "role":  "Well Performance Engineer",
            "email": "wellperf@redbluffresources.com"
        }
    }

def daily_dates(start):
    dates = []
    d = start
    while d <= END_DATE:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates

def nr(lo, hi, noise_pct=0.04):
    mid = (lo + hi) / 2
    noise = (hi - lo) * noise_pct
    return round(float(np.clip(np.random.normal(mid, noise), lo, hi)), 2)

# ── SUPPORTING ASSET HISTORY BUILDERS ────────────────────────────────────────
def build_facility(dates):
    h = []
    oil = random.uniform(500,3000); water = random.uniform(500,5000); gas = random.uniform(200,2000)
    for ds in dates:
        oil   = max(50, oil   + np.random.normal(0, oil*0.02))
        water = max(50, water + np.random.normal(0, water*0.02))
        gas   = max(10, gas   + np.random.normal(0, gas*0.02))
        h.append({"date":ds,"tester_pressure_psi":nr(20,250),"tester_temp_f":nr(60,220),
                  "tester_oil_rate_bpd":round(nr(50,2000),2),"tester_water_rate_bpd":round(nr(50,3000),2),
                  "tester_gas_rate_mcfd":round(nr(10,500),2),"yesterday_oil_bbl":round(oil,2),
                  "yesterday_water_bbl":round(water,2),"yesterday_gas_mcf":round(gas,2),
                  "gas_sales_rate_mscfd":round(nr(100,8000),2),"gas_sales_temp_f":nr(40,120),
                  "gas_sales_pressure_psi":nr(20,150),"lact_flow_rate_bblhr":round(nr(50,800),2),
                  "lact_temp_f":nr(60,180),"lact_density_api":nr(28,42),"lact_bsw_pct":round(nr(0.5,8.0),2),
                  "water_transfer_rate_bpd":round(nr(100,5000),2),"water_discharge_psi":nr(50,280),
                  "water_temp_f":nr(50,150),"oil_tank_level_ft":nr(2.0,22.0),
                  "water_tank_level_ft":nr(1.0,18.0),"run_status":"ONLINE"})
    return h

def build_compressor(dates):
    h = []
    for ds in dates:
        h.append({"date":ds,"suction_pressure_psi":nr(50,200),"suction_temp_f":nr(40,120),
                  "discharge_pressure_psi":nr(800,1400),"discharge_temp_f":nr(100,280),
                  "throughput_mscfd":round(nr(500,15000),2),"engine_rpm":round(nr(900,1200),0),
                  "fuel_gas_consumption_mcfd":round(nr(20,200),2),"engine_temp_f":nr(180,350),
                  "lube_oil_pressure_psi":nr(30,80),"runtime_hrs":round(nr(20,24),1),"run_status":"ONLINE"})
    return h

def build_swd(dates):
    h = []
    vol = random.uniform(500,8000); cum = 0
    for ds in dates:
        vol = max(50, vol + np.random.normal(0, vol*0.03)); cum += vol
        h.append({"date":ds,"injection_rate_bpd":round(vol,2),"injection_pressure_psi":nr(100,1800),
                  "wellhead_pressure_psi":nr(50,400),"tubing_pressure_psi":nr(80,600),
                  "casing_pressure_psi":nr(20,200),"injection_temp_f":nr(60,160),
                  "daily_volume_injected_bbl":round(vol,2),"cumulative_volume_bbl":round(cum,2),
                  "pump_discharge_pressure_psi":nr(150,2000),"pump_speed_hz":nr(40,60),"run_status":"ONLINE"})
    return h

def build_tank(dates):
    h = []
    oil = random.uniform(5.0,18.0); water = random.uniform(3.0,15.0)
    for ds in dates:
        oil   = float(np.clip(oil   + np.random.normal(0,0.8), 1.0, 28.0))
        water = float(np.clip(water + np.random.normal(0,1.0), 1.0, 28.0))
        h.append({"date":ds,"oil_tank_level_ft":round(oil,2),"water_tank_level_ft":round(water,2),
                  "oil_tank_temp_f":nr(50,120),"water_tank_temp_f":nr(50,100),
                  "oil_hauled_today_bbl":round(nr(0,500),2),"water_hauled_today_bbl":round(nr(0,1000),2),
                  "high_tank_alarm":1 if oil>26.0 or water>26.0 else 0,"run_status":"ONLINE"})
    return h

# ── SUPPORTING ASSET INVOICE LIBRARIES ───────────────────────────────────────
EXPENSE_MAP = {
    "Facility": {
        "Monthly":[
            {"desc":"Facility Electrical Power","gl":"605-110","range":(800,2500),"vendor":"Trans-Pecos Electric"},
            {"desc":"Production Technician Labor Allocation","gl":"605-210","range":(800,1200),"vendor":"Red Bluff Internal"},
            {"desc":"Chemical Treatment - Facility","gl":"605-310","range":(300,800),"vendor":"Permian Chemical Supply"},
            {"desc":"SCADA / RTU Telemetry - Facility","gl":"605-410","range":(150,300),"vendor":"Basin Telemetry & Controls"},
            {"desc":"G&A Overhead Allocation","gl":"822-110","range":(300,800),"vendor":"Red Bluff Internal"},
        ],
        "Ad_Hoc":[
            {"desc":"LACT Unit Calibration & Service","gl":"605-710","range":(500,1500),"vendor":"West Texas Electrical Services","prob":0.25},
            {"desc":"Separator / Tester Maintenance","gl":"605-720","range":(400,1200),"vendor":"Pecos Valley Roustabout","prob":0.20},
            {"desc":"Roustabout - Facility Maintenance","gl":"605-710","range":(300,700),"vendor":"Pecos Valley Roustabout","prob":0.30},
        ]
    },
    "Compressor": {
        "Monthly":[
            {"desc":"Compression Fuel Gas","gl":"605-120","range":(3000,12000),"vendor":"Red Bluff Internal"},
            {"desc":"Production Technician Labor Allocation","gl":"605-210","range":(800,1200),"vendor":"Red Bluff Internal"},
            {"desc":"Compressor Lube Oil","gl":"605-310","range":(400,900),"vendor":"Permian Chemical Supply"},
            {"desc":"SCADA / RTU Telemetry - Compression","gl":"605-410","range":(150,300),"vendor":"Basin Telemetry & Controls"},
            {"desc":"G&A Overhead Allocation","gl":"822-110","range":(300,800),"vendor":"Red Bluff Internal"},
        ],
        "Ad_Hoc":[
            {"desc":"Compressor Skid - PM Service","gl":"605-720","range":(900,2500),"vendor":"Delaware Basin Compression","prob":0.35},
            {"desc":"Engine Filter & Belt Replacement","gl":"605-720","range":(300,800),"vendor":"Delaware Basin Compression","prob":0.25},
            {"desc":"Compressor Valve Overhaul","gl":"605-720","range":(2000,6000),"vendor":"Delaware Basin Compression","prob":0.10},
            {"desc":"Freeze Protection - Methanol Injection","gl":"605-320","range":(800,2000),"vendor":"Permian Chemical Supply","prob":0.15},
        ]
    },
    "SWD Well": {
        "Monthly":[
            {"desc":"SWD Injection Pump - Electrical Power","gl":"605-130","range":(1500,5000),"vendor":"Trans-Pecos Electric"},
            {"desc":"Production Technician Labor Allocation","gl":"605-210","range":(800,1200),"vendor":"Red Bluff Internal"},
            {"desc":"Chemical Treatment - Scale/Corrosion","gl":"605-310","range":(400,900),"vendor":"Permian Chemical Supply"},
            {"desc":"SCADA / RTU Telemetry - SWD","gl":"605-410","range":(150,250),"vendor":"Basin Telemetry & Controls"},
            {"desc":"G&A Overhead Allocation","gl":"822-110","range":(300,800),"vendor":"Red Bluff Internal"},
        ],
        "Ad_Hoc":[
            {"desc":"SWD Disposal Fee - Volume Overage","gl":"605-520","range":(1000,4000),"vendor":"Reeves County Water Hauling","prob":0.30},
            {"desc":"Injection Pump Maintenance","gl":"605-740","range":(500,1500),"vendor":"West Texas Electrical Services","prob":0.25},
            {"desc":"Wellbore Pressure Test - Regulatory","gl":"605-710","range":(800,2000),"vendor":"Red Bluff Rig Services","prob":0.10},
            {"desc":"Acid Treatment - SWD Wellbore","gl":"605-620","range":(2000,5000),"vendor":"Permian Chemical Supply","prob":0.08},
        ]
    },
    "Tank Battery": {
        "Monthly":[
            {"desc":"Produced Water Hauling - Tank Battery","gl":"605-510","range":(2000,8000),"vendor":"Reeves County Water Hauling"},
            {"desc":"Production Technician Labor Allocation","gl":"605-210","range":(800,1200),"vendor":"Red Bluff Internal"},
            {"desc":"Tank Battery Chemical Treatment","gl":"605-310","range":(300,700),"vendor":"Permian Chemical Supply"},
            {"desc":"SCADA / RTU Telemetry - Tank Battery","gl":"605-410","range":(150,250),"vendor":"Basin Telemetry & Controls"},
            {"desc":"G&A Overhead Allocation","gl":"822-110","range":(300,800),"vendor":"Red Bluff Internal"},
        ],
        "Ad_Hoc":[
            {"desc":"Oil Haul-Off - Tank Battery","gl":"605-510","range":(500,2000),"vendor":"Reeves County Water Hauling","prob":0.70},
            {"desc":"Tank Cleaning & Inspection","gl":"605-710","range":(800,2500),"vendor":"Pecos Valley Roustabout","prob":0.15},
            {"desc":"Tank Vent / Thief Hatch Repair","gl":"605-710","range":(200,600),"vendor":"Pecos Valley Roustabout","prob":0.20},
            {"desc":"Hot Oil Treatment - Tank Battery","gl":"605-610","range":(600,1500),"vendor":"Midland Hot Oil Service","prob":0.20},
        ]
    },
}

def gen_invoices(asset_id, history, lib):
    invoices = []
    for day in history:
        ds = day["date"]
        if ds.endswith("-28"):
            for exp in lib["Monthly"]:
                invoices.append((f"RBNR-{ds[:4]}-{random.randint(10000,99999)}", ds,
                                  "Monthly LOE", exp["gl"], exp["desc"], exp["vendor"],
                                  asset_id, round(random.uniform(*exp["range"]), 2)))
            for exp in lib["Ad_Hoc"]:
                if random.random() < exp["prob"]:
                    invoices.append((f"RBNR-{ds[:4]}-{random.randint(10000,99999)}", ds,
                                      "Ad-Hoc LOE", exp["gl"], exp["desc"], exp["vendor"],
                                      asset_id, round(random.uniform(*exp["range"]), 2)))
    return invoices

# ── CLEAN WELL REBUILD ────────────────────────────────────────────────────────
SENSOR_NORMALS_CLEAN = {
    "Pumping Unit": {"gross_stroke_len":(120.0,168.0),"net_stroke_len":(100.0,148.0),
                     "strokes_per_min":(0.5,12.0),"pump_fillage_pct":(0.0,100.0),
                     "casing_pressure_psi":(50.0,5000.0),"tubing_pressure_psi":(50.0,150.0),
                     "motor_current_amps":(10.0,200.0),"freq_hz":(57.0,62.0),
                     "pump_intake_pressure":(0.0,5000.0),"motor_temp_f":(100.0,160.0)},
    "ESP":          {"avg_tubing_pressure":(200.0,500.0),"avg_casing_pressure":(20.0,200.0),
                     "motor_temp_f":(150.0,250.0),"motor_current_amps":(10.0,200.0),
                     "freq_hz":(0.0,100.0),"pump_intake_pressure_psi":(0.0,5000.0),
                     "pump_discharge_pressure_psi":(0.0,5000.0),"vibration_hz":(0.0,1.25)},
    "Gas Lift":     {"avg_casing_pressure":(50.0,5000.0),"casing_injection_pressure_psi":(800.0,1200.0),
                     "tubing_pressure_psi":(50.0,150.0),"injection_rate_mscfd":(0.0,50000.0),
                     "wellhead_temp_f":(10.0,400.0),"choke_position_pct":(0.0,100.0),
                     "motor_temp_f":(80.0,130.0)},
}

PROD_PARAMS = {
    "Pumping Unit": {"water_base":(50.0,800.0),"gas_base":(10.0,150.0),"water_noise":0.06,"gas_noise":0.05,"water_decline":0.0002,"gas_decline":0.0008,"water_rise":0.0001},
    "ESP":          {"water_base":(200.0,5000.0),"gas_base":(50.0,500.0),"water_noise":0.05,"gas_noise":0.04,"water_decline":0.0001,"gas_decline":0.0007,"water_rise":0.00015},
    "Gas Lift":     {"water_base":(100.0,2000.0),"gas_base":(100.0,2000.0),"water_noise":0.07,"gas_noise":0.06,"water_decline":0.00015,"gas_decline":0.0006,"water_rise":0.00012},
}

def rebuild_clean_history(original_rows, lift):
    """Return a list of clean dicts for rb_well_history (no failures)."""
    normals     = SENSOR_NORMALS_CLEAN[lift]
    sensor_keys = list(normals.keys())
    days        = len(original_rows)
    t           = np.arange(days)
    pp          = PROD_PARAMS[lift]
    water_base  = random.uniform(*pp["water_base"])
    gas_base    = random.uniform(*pp["gas_base"])
    water_rates = np.clip(
        water_base * (1 + pp["water_rise"]*t) * np.exp(-pp["water_decline"]*t)
        * np.random.normal(1.0, pp["water_noise"], days), 1.0, None)
    gas_rates   = np.clip(
        gas_base * np.exp(-pp["gas_decline"]*t)
        * np.random.normal(1.0, pp["gas_noise"], days), 0.5, None)

    test_day = next((datetime.strptime(str(r[1])[:10], "%Y-%m-%d").day
                     for r in original_rows if r[2] is not None), random.randint(5,25))

    new_rows = []
    for i, orig in enumerate(original_rows):
        date_str   = str(orig[1])[:10]  # date column
        allocated  = max(float(orig[2] or 5.0), 5.0)
        is_test    = datetime.strptime(date_str, "%Y-%m-%d").day == test_day
        row = {
            "date":            date_str,
            "allocated_oil":   allocated,
            "test_oil":        round(allocated*(1+random.choice([1,-1])*random.uniform(0.05,0.12)),2) if is_test else None,
            "allocated_water": round(float(water_rates[i]),2),
            "allocated_gas":   round(float(gas_rates[i]),2),
            "run_status":      "PRODUCING",
            "failure_cause":   "",
        }
        for k in ['gross_stroke_len','net_stroke_len']:
            if k in normals:
                lo, hi = normals[k]
                mid = (lo+hi)/2
                row[k] = round(float(np.clip(np.random.normal(mid, mid*0.01), lo, hi)), 2)
        for s in sensor_keys:
            if s in ('gross_stroke_len','net_stroke_len'):
                continue
            lo, hi = normals[s]
            mid    = (lo+hi)/2
            noise  = (hi-lo)*0.04
            row[s] = round(float(np.clip(np.random.normal(mid, noise), lo, hi)), 2)
        new_rows.append(row)
    return new_rows

def _bulk_insert_invoices(cursor, rows):
    for i in range(0, len(rows), CHUNK):
        execute_values(cursor, """
            INSERT INTO rb_invoices
                (invoice_id, invoice_date, category, gl_code,
                 service_description, vendor, asset_id, total_usd)
            VALUES %s
        """, rows[i:i+CHUNK])

def _bulk_insert_well_history(cursor, asset_id, rows):
    db_rows = []
    for row in rows:
        db_rows.append((
            asset_id, row["date"],
            row.get("allocated_oil"),  row.get("test_oil"),
            row.get("allocated_water"), row.get("allocated_gas"),
            row.get("gross_stroke_len"), row.get("net_stroke_len"),
            row.get("strokes_per_min"),  row.get("pump_fillage_pct"),
            row.get("casing_pressure_psi"), row.get("tubing_pressure_psi"),
            row.get("motor_current_amps"),  row.get("freq_hz"),
            row.get("pump_intake_pressure"), row.get("motor_temp_f"),
            row.get("avg_tubing_pressure"),         row.get("avg_casing_pressure"),
            row.get("pump_intake_pressure_psi"),    row.get("pump_discharge_pressure_psi"),
            row.get("vibration_hz"),
            row.get("casing_injection_pressure_psi"), row.get("injection_rate_mscfd"),
            row.get("wellhead_temp_f"),                row.get("choke_position_pct"),
            row.get("run_status"), row.get("failure_cause", ""),
        ))
    for i in range(0, len(db_rows), CHUNK):
        execute_values(cursor, """
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
        """, db_rows[i:i+CHUNK])

if __name__ == "__main__":
    print("Red Bluff Resources — Master Builder")
    print("─" * 60)
    conn = psycopg2.connect(
    dbname="oilfield",
    user="postgres",
    password="postgres",
    host="127.0.0.1",
    port="5433",
    )
    cursor = conn.cursor()


    # ── 1. Assign location and personnel to all existing wells ────────────────
    print("\n[1/4] Assigning location and personnel to wells...")
    cursor.execute("SELECT asset_id, lift FROM rb_assets WHERE type = 'Well'")
    wells = cursor.fetchall()

    for asset_id, lift in wells:
        lease = asset_id.split('-')[0]
        loc   = LEASE_LOCATION[lease]
        cursor.execute("""
            UPDATE rb_assets
            SET basin=%s, sub_basin=%s, state=%s, county=%s, region=%s
            WHERE asset_id=%s
        """, (loc["basin"], loc["sub_basin"], loc["state"],
              loc["county"], loc["region"], asset_id))
        # Upsert correct personnel
        for role_key, person in get_personnel(lease).items():
            cursor.execute("""
                INSERT INTO rb_personnel (asset_id, role_key, name, role, email)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (asset_id, role_key)
                DO UPDATE SET name=EXCLUDED.name, role=EXCLUDED.role, email=EXCLUDED.email
            """, (asset_id, role_key, person["name"], person["role"], person["email"]))
    conn.commit()
    print(f"  Updated {len(wells)} wells")

    # ── 2. Find earliest well date per lease ──────────────────────────────────
    print("\n[2/4] Finding earliest dates per lease...")
    lease_starts = {}
    for lease in LEASES:
        cursor.execute("""
            SELECT MIN(h.date) FROM rb_well_history h
            JOIN rb_assets a ON a.asset_id = h.asset_id
            WHERE a.type = 'Well' AND a.asset_id LIKE %s
        """, (f"{lease}-%",))
        result = cursor.fetchone()[0]
        lease_starts[lease] = result
        print(f"  {lease}: {result}")

    # ── 3. Select 12 clean wells (3 per lease) and rebuild as clean ───────────
    print("\n[3/4] Selecting and rebuilding 12 clean wells...")
    for lease in LEASES:
        cursor.execute("""
            SELECT a.asset_id, a.lift,
                   COUNT(CASE WHEN h.run_status='SHUT-IN' THEN 1 END) AS shutins
            FROM rb_assets a
            JOIN rb_well_history h ON h.asset_id = a.asset_id
            WHERE a.type='Well' AND a.asset_id LIKE %s
            GROUP BY a.asset_id, a.lift
            ORDER BY shutins ASC
        """, (f"{lease}-%",))
        ranked = cursor.fetchall()

        chosen = {}
        for asset_id, lift, _ in ranked:
            if lift not in chosen:
                chosen[lift] = asset_id
            if len(chosen) == 3:
                break

        for lift, asset_id in chosen.items():
            cursor.execute("""
                SELECT asset_id, date, allocated_oil, run_status
                FROM rb_well_history
                WHERE asset_id=%s
                ORDER BY date
            """, (asset_id,))
            orig_rows = cursor.fetchall()

            clean_rows = rebuild_clean_history(orig_rows, lift)

            # Replace history: delete + re-insert
            cursor.execute("DELETE FROM rb_well_history WHERE asset_id=%s", (asset_id,))
            _bulk_insert_well_history(cursor, asset_id, clean_rows)
            # Remove workover invoices for clean wells
            cursor.execute("DELETE FROM rb_invoices WHERE asset_id=%s AND category='Workover'", (asset_id,))
            print(f"  ✓ {asset_id} ({lift}) — rebuilt as clean ({len(clean_rows)} days)")

    conn.commit()

    # ── 4. Add 16 supporting assets (4 per lease) ────────────────────────────
    print("\n[4/4] Adding supporting assets...")
    BUILDERS = [
        ("Facility",     "FAC-01",  build_facility,  "rb_facility_history"),
        ("Compressor",   "COMP-01", build_compressor,"rb_compressor_history"),
        ("SWD Well",     "SWD-01",  build_swd,       "rb_swd_history"),
        ("Tank Battery", "TANK-01", build_tank,       "rb_tank_battery_history"),
    ]

    INSERT_SQL = {
        "rb_facility_history": """
            INSERT INTO rb_facility_history
                (asset_id, date, tester_pressure_psi, tester_temp_f, tester_oil_rate_bpd,
                 tester_water_rate_bpd, tester_gas_rate_mcfd, yesterday_oil_bbl,
                 yesterday_water_bbl, yesterday_gas_mcf, gas_sales_rate_mscfd,
                 gas_sales_temp_f, gas_sales_pressure_psi, lact_flow_rate_bblhr,
                 lact_temp_f, lact_density_api, lact_bsw_pct,
                 water_transfer_rate_bpd, water_discharge_psi, water_temp_f,
                 oil_tank_level_ft, water_tank_level_ft, run_status)
            VALUES %s
        """,
        "rb_compressor_history": """
            INSERT INTO rb_compressor_history
                (asset_id, date, suction_pressure_psi, suction_temp_f,
                 discharge_pressure_psi, discharge_temp_f, throughput_mscfd,
                 engine_rpm, fuel_gas_consumption_mcfd, engine_temp_f,
                 lube_oil_pressure_psi, runtime_hrs, run_status)
            VALUES %s
        """,
        "rb_swd_history": """
            INSERT INTO rb_swd_history
                (asset_id, date, injection_rate_bpd, injection_pressure_psi,
                 wellhead_pressure_psi, tubing_pressure_psi, casing_pressure_psi,
                 injection_temp_f, daily_volume_injected_bbl, cumulative_volume_bbl,
                 pump_discharge_pressure_psi, pump_speed_hz, run_status)
            VALUES %s
        """,
        "rb_tank_battery_history": """
            INSERT INTO rb_tank_battery_history
                (asset_id, date, oil_tank_level_ft, water_tank_level_ft,
                 oil_tank_temp_f, water_tank_temp_f, oil_hauled_today_bbl,
                 water_hauled_today_bbl, high_tank_alarm, run_status)
            VALUES %s
        """,
    }

    ROW_MAPPERS = {
        "rb_facility_history":    lambda aid, d: (
            aid, d["date"], d["tester_pressure_psi"], d["tester_temp_f"],
            d["tester_oil_rate_bpd"], d["tester_water_rate_bpd"], d["tester_gas_rate_mcfd"],
            d["yesterday_oil_bbl"], d["yesterday_water_bbl"], d["yesterday_gas_mcf"],
            d["gas_sales_rate_mscfd"], d["gas_sales_temp_f"], d["gas_sales_pressure_psi"],
            d["lact_flow_rate_bblhr"], d["lact_temp_f"], d["lact_density_api"],
            d["lact_bsw_pct"], d["water_transfer_rate_bpd"], d["water_discharge_psi"],
            d["water_temp_f"], d["oil_tank_level_ft"], d["water_tank_level_ft"], d["run_status"]
        ),
        "rb_compressor_history":  lambda aid, d: (
            aid, d["date"], d["suction_pressure_psi"], d["suction_temp_f"],
            d["discharge_pressure_psi"], d["discharge_temp_f"], d["throughput_mscfd"],
            d["engine_rpm"], d["fuel_gas_consumption_mcfd"], d["engine_temp_f"],
            d["lube_oil_pressure_psi"], d["runtime_hrs"], d["run_status"]
        ),
        "rb_swd_history":         lambda aid, d: (
            aid, d["date"], d["injection_rate_bpd"], d["injection_pressure_psi"],
            d["wellhead_pressure_psi"], d["tubing_pressure_psi"], d["casing_pressure_psi"],
            d["injection_temp_f"], d["daily_volume_injected_bbl"], d["cumulative_volume_bbl"],
            d["pump_discharge_pressure_psi"], d["pump_speed_hz"], d["run_status"]
        ),
        "rb_tank_battery_history": lambda aid, d: (
            aid, d["date"], d["oil_tank_level_ft"], d["water_tank_level_ft"],
            d["oil_tank_temp_f"], d["water_tank_temp_f"], d["oil_hauled_today_bbl"],
            d["water_hauled_today_bbl"], d["high_tank_alarm"], d["run_status"]
        ),
    }

    for lease in LEASES:
        start    = lease_starts[lease]
        dates    = daily_dates(start if isinstance(start, datetime)
                               else datetime.strptime(str(start)[:10], "%Y-%m-%d"))
        loc      = LEASE_LOCATION[lease]
        pers     = get_personnel(lease)

        for atype, suffix, builder_fn, table in BUILDERS:
            aid     = f"{lease}-{suffix}"
            history = builder_fn(dates)
            invoices = gen_invoices(aid, history, EXPENSE_MAP[atype])

            # rb_assets
            cursor.execute("""
                INSERT INTO rb_assets (asset_id, type, lift, basin, sub_basin, state, county, region)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (asset_id) DO UPDATE
                  SET type=EXCLUDED.type, basin=EXCLUDED.basin, sub_basin=EXCLUDED.sub_basin,
                      state=EXCLUDED.state, county=EXCLUDED.county, region=EXCLUDED.region
            """, (aid, atype, None, loc["basin"], loc["sub_basin"],
                  loc["state"], loc["county"], loc["region"]))

            # rb_personnel
            for role_key, person in pers.items():
                cursor.execute("""
                    INSERT INTO rb_personnel (asset_id, role_key, name, role, email)
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (asset_id, role_key)
                    DO UPDATE SET name=EXCLUDED.name, role=EXCLUDED.role, email=EXCLUDED.email
                """, (aid, role_key, person["name"], person["role"], person["email"]))

            # history table
            mapper  = ROW_MAPPERS[table]
            db_rows = [mapper(aid, d) for d in history]
            for i in range(0, len(db_rows), CHUNK):
                execute_values(cursor, INSERT_SQL[table], db_rows[i:i+CHUNK])

            # invoices
            _bulk_insert_invoices(cursor, invoices)
            print(f"  {aid:<32} {len(history):>6} days  {len(invoices):>6} invoices")

    conn.commit()
    conn.close()
    print("\n✓ Done — location, personnel, supporting assets, invoices all written to PostgreSQL")