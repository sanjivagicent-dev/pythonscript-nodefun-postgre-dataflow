"""
Red Bluff Resources — Invoice Generator
========================================
Reads well history from rb_well_history (PostgreSQL) and generates:
  - Monthly LOE invoices (on the 28th of each month)
  - Ad-hoc LOE invoices (probabilistic)
  - Workover invoices (triggered on PRODUCING day after SHUT-IN)

Pipeline order:
  1. python3 generate_digital_twin.py   → rb_assets, rb_personnel, rb_well_history
  2. python3 generate_invoices.py       → THIS SCRIPT → rb_invoices (wells only)
  3. python3 build_master.py            → adds location, supporting assets + their invoices
  4. python3 export_to_parquet.py       → exports all RB tables to .parquet

Output: PostgreSQL table rb_invoices (well rows only; supporting assets added by build_master.py)
"""

import random
import psycopg2
from psycopg2.extras import execute_values

# ── DB connection ─────────────────────────────────────────────────────────────
DB_CONFIG = dict(dbname="oilfield", user="postgres", password="postgres",
                 host="127.0.0.1", port="5433")

EXPENSE_LIBRARY = {
    "Pumping Unit": {
        "Monthly": [
            {"desc": "Electrical Power - Surface",             "gl": "605-110", "range": (1200, 2800),  "vendor": "Trans-Pecos Electric"},
            {"desc": "Production Technician Labor Allocation", "gl": "605-210", "range": (800,  1200),  "vendor": "Red Bluff Internal"},
            {"desc": "Chemical Treatment - Corrosion/Scale",   "gl": "605-310", "range": (400,  900),   "vendor": "Permian Chemical Supply"},
            {"desc": "SCADA / RTU Telemetry",                  "gl": "605-410", "range": (150,  250),   "vendor": "Basin Telemetry & Controls"},
            {"desc": "G&A Overhead Allocation",                "gl": "822-110", "range": (300,  800),   "vendor": "Red Bluff Internal"},
        ],
        "Ad_Hoc": [
            {"desc": "Produced Water Hauling",           "gl": "605-510", "range": (1500, 4000), "vendor": "Reeves County Water Hauling", "prob": 0.85},
            {"desc": "Hot Oil Treatment - Paraffin",     "gl": "605-610", "range": (800,  1500), "vendor": "Midland Hot Oil Service",      "prob": 0.30},
            {"desc": "Roustabout - Location Maintenance","gl": "605-710", "range": (300,  600),  "vendor": "Pecos Valley Roustabout",      "prob": 0.15},
        ],
        "Workover": {
            "desc": "Rod Job: Parted Rods / Pump R&R",
            "gl":   "710-520",
            "vendor": "Red Bluff Rig Services",
            "rig_rate": (550, 750), "rig_hours": (12, 24), "parts": (2000, 5000),
        }
    },
    "ESP": {
        "Monthly": [
            {"desc": "Electrical Power - High Draw",             "gl": "605-130", "range": (5000, 12000), "vendor": "Trans-Pecos Electric"},
            {"desc": "Production Technician Labor Allocation",   "gl": "605-210", "range": (800,  1200),  "vendor": "Red Bluff Internal"},
            {"desc": "Chemical Treatment - ESP Scale Inhibitor", "gl": "605-330", "range": (1000, 2500),  "vendor": "Permian Chemical Supply"},
            {"desc": "VFD Monitoring & Controls",                "gl": "605-420", "range": (300,  600),   "vendor": "Basin Telemetry & Controls"},
            {"desc": "G&A Overhead Allocation",                  "gl": "822-110", "range": (300,  800),   "vendor": "Red Bluff Internal"},
        ],
        "Ad_Hoc": [
            {"desc": "SWD Disposal Fee - High Volume",  "gl": "605-520", "range": (4000, 9000), "vendor": "Reeves County Water Hauling",    "prob": 0.90},
            {"desc": "VFD Calibration & Reset",         "gl": "605-740", "range": (450,  900),  "vendor": "West Texas Electrical Services", "prob": 0.25},
            {"desc": "Acid Treatment - Batch",          "gl": "605-620", "range": (2500, 5000), "vendor": "Permian Chemical Supply",        "prob": 0.15},
        ],
        "Workover": {
            "desc": "ESP Workover: Motor & Cable R&R",
            "gl":   "710-520",
            "vendor": "Red Bluff Rig Services",
            "rig_rate": (550, 750), "rig_hours": (36, 72),
            "motor": (45000, 70000), "cable": (18000, 28000),
        }
    },
    "Gas Lift": {
        "Monthly": [
            {"desc": "Compression Fuel / Power",              "gl": "605-120", "range": (2000, 4500), "vendor": "Trans-Pecos Electric"},
            {"desc": "Production Technician Labor Allocation","gl": "605-210", "range": (800,  1200), "vendor": "Red Bluff Internal"},
            {"desc": "Chemical Injection - Continuous",       "gl": "605-310", "range": (600,  1100), "vendor": "Permian Chemical Supply"},
            {"desc": "SCADA / RTU Telemetry",                 "gl": "605-410", "range": (150,  250),  "vendor": "Basin Telemetry & Controls"},
            {"desc": "G&A Overhead Allocation",               "gl": "822-110", "range": (300,  800),  "vendor": "Red Bluff Internal"},
        ],
        "Ad_Hoc": [
            {"desc": "Produced Water Hauling",              "gl": "605-510", "range": (1500, 4000), "vendor": "Reeves County Water Hauling",  "prob": 0.85},
            {"desc": "Compressor Skid - PM Service",        "gl": "605-720", "range": (900,  1800), "vendor": "Delaware Basin Compression",   "prob": 0.35},
            {"desc": "Freeze Protection - Methanol Injection","gl":"605-320", "range": (1200, 2500), "vendor": "Permian Chemical Supply",     "prob": 0.15},
        ],
        "Workover": {
            "desc": "Slickline - Gas Lift Valve R&R",
            "gl":   "710-520",
            "vendor": "Red Bluff Rig Services",
            "rig_rate": (400, 550), "rig_hours": (8, 16), "parts": (3000, 7000),
        }
    }
}

def build_workover_invoice(asset_id, date_str, lift):
    lib  = EXPENSE_LIBRARY[lift]["Workover"]
    rate = random.uniform(*lib["rig_rate"])
    hrs  = random.uniform(*lib["rig_hours"])
    cost = rate * hrs
    if "motor" in lib:
        cost += random.uniform(*lib["motor"]) + random.uniform(*lib["cable"])
    elif "parts" in lib:
        cost += random.uniform(*lib["parts"])
    return {
        "invoice_id":          f"RBNR-{date_str[:4]}-{random.randint(10000,99999)}",
        "invoice_date":        date_str,
        "category":            "Workover",
        "gl_code":             lib["gl"],
        "service_description": lib["desc"],
        "vendor":              lib["vendor"],
        "total_usd":           round(cost, 2),
        "asset_id":            asset_id,
    }

CHUNK = 5000

def _bulk_insert(cursor, rows):
    if not rows:
        return
    for i in range(0, len(rows), CHUNK):
        execute_values(cursor, """
            INSERT INTO rb_invoices
                (invoice_id, invoice_date, category, gl_code,
                 service_description, vendor, asset_id, total_usd)
            VALUES %s
        """, [(r["invoice_id"], r["invoice_date"], r["category"], r["gl_code"],
               r["service_description"], r["vendor"], r["asset_id"], r["total_usd"])
              for r in rows[i:i+CHUNK]])

if __name__ == "__main__":
    print("Generating Well Financial Ledger...")
    conn = psycopg2.connect(
        dbname="oilfield",
        user="postgres",
        password="postgres",
        host="127.0.0.1",
        port="5433",
    )
    cursor = conn.cursor()

    # Load all well assets from DB
    cursor.execute("SELECT asset_id, lift FROM rb_assets WHERE type = 'Well'")
    well_assets = cursor.fetchall()
    print(f"  Found {len(well_assets)} wells in rb_assets")

    invoices   = []
    total_rows = 0

    for asset_id, lift in well_assets:
        lib = EXPENSE_LIBRARY[lift]

        cursor.execute("""
            SELECT date, run_status
            FROM rb_well_history
            WHERE asset_id = %s
            ORDER BY date
        """, (asset_id,))
        rows = cursor.fetchall()
        total_rows += len(rows)

        fail_start = None
        for date_val, run_status in rows:
            date_str = date_val.strftime("%Y-%m-%d") if hasattr(date_val, "strftime") else str(date_val)

            # Monthly LOE on the 28th
            if date_str.endswith("-28"):
                for exp in lib["Monthly"]:
                    invoices.append({
                        "invoice_id":          f"RBNR-{date_str[:4]}-{random.randint(10000,99999)}",
                        "invoice_date":        date_str,
                        "category":            "Monthly LOE",
                        "gl_code":             exp["gl"],
                        "service_description": exp["desc"],
                        "vendor":              exp["vendor"],
                        "total_usd":           round(random.uniform(*exp["range"]), 2),
                        "asset_id":            asset_id,
                    })
                for exp in lib["Ad_Hoc"]:
                    if random.random() < exp["prob"]:
                        invoices.append({
                            "invoice_id":          f"RBNR-{date_str[:4]}-{random.randint(10000,99999)}",
                            "invoice_date":        date_str,
                            "category":            "Ad-Hoc LOE",
                            "gl_code":             exp["gl"],
                            "service_description": exp["desc"],
                            "vendor":              exp["vendor"],
                            "total_usd":           round(random.uniform(*exp["range"]), 2),
                            "asset_id":            asset_id,
                        })

            # Workover on return to PRODUCING after SHUT-IN
            if run_status == "SHUT-IN" and fail_start is None:
                fail_start = date_str
            elif run_status == "PRODUCING" and fail_start is not None:
                invoices.append(build_workover_invoice(asset_id, date_str, lift))
                fail_start = None

    print(f"  Well history rows scanned: {total_rows:,}")
    print(f"  Invoices generated:        {len(invoices):,}")
    print("  Writing to rb_invoices...")
    _bulk_insert(cursor, invoices)
    conn.commit()
    conn.close()
    print("✓ Done — rb_invoices populated (well rows)")