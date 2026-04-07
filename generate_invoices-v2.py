import random
import psycopg2
import io
from collections import defaultdict

# ---------------- DB CONNECTION ----------------
conn = psycopg2.connect(
    dbname="oilfield",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5433"
)

# ---------------- EXPENSE LIBRARY ----------------
EXPENSE_LIBRARY = {
    "Rod Pump": {
        "Monthly": [
            {"desc": "Utility / Electrical Power", "gl": "605110", "range": (1200, 2800), "vendor": "Permian Power Co."},
            {"desc": "Lease Operator Allocation", "gl": "605210", "range": (800, 1200), "vendor": "Apex Internal"},
            {"desc": "Corrosion / Scale Inhibitor", "gl": "605310", "range": (400, 900), "vendor": "WestTex Chemicals"},
            {"desc": "SCADA / Cellular Telemetry", "gl": "605410", "range": (150, 250), "vendor": "Oilfield Tech Solutions"},
            {"desc": "Corporate G&A Overhead", "gl": "822110", "range": (300, 800), "vendor": "Apex Internal"}
        ],
        "Ad_Hoc": [
            {"desc": "Produced Water Hauling", "gl": "605510", "range": (1500, 4000), "vendor": "Blue Water Logistics", "prob": 0.85},
            {"desc": "Hot Oiling / Paraffin Treatment", "gl": "605610", "range": (800, 1500), "vendor": "Hot Oil Services LLC", "prob": 0.30},
            {"desc": "Roustabout: Site Maintenance", "gl": "605710", "range": (300, 600), "vendor": "WestTex Roustabout", "prob": 0.15}
        ]
    },
    "Gas Lift": {
        "Monthly": [
            {"desc": "Compressor Fuel / Electricity", "gl": "605120", "range": (2000, 4500), "vendor": "Permian Power Co."},
            {"desc": "Lease Operator Allocation", "gl": "605210", "range": (800, 1200), "vendor": "Apex Internal"},
            {"desc": "Continuous Chemical Injection", "gl": "605310", "range": (600, 1100), "vendor": "WestTex Chemicals"},
            {"desc": "SCADA / Cellular Telemetry", "gl": "605410", "range": (150, 250), "vendor": "Oilfield Tech Solutions"},
            {"desc": "Corporate G&A Overhead", "gl": "822110", "range": (300, 800), "vendor": "Apex Internal"}
        ],
        "Ad_Hoc": [
            {"desc": "Produced Water Hauling", "gl": "605510", "range": (1500, 4000), "vendor": "Blue Water Logistics", "prob": 0.85},
            {"desc": "Compressor Skid Maintenance", "gl": "605720", "range": (900, 1800), "vendor": "Apex Compression Services", "prob": 0.35},
            {"desc": "Winterization: Methanol", "gl": "605320", "range": (1200, 2500), "vendor": "WestTex Chemicals", "prob": 0.15}
        ]
    },
    "ESP": {
        "Monthly": [
            {"desc": "Utility / Heavy Electrical Power", "gl": "605130", "range": (5000, 12000), "vendor": "Permian Power Co."},
            {"desc": "Lease Operator Allocation", "gl": "605210", "range": (800, 1200), "vendor": "Apex Internal"},
            {"desc": "Specialized Scale Inhibitor", "gl": "605330", "range": (1000, 2500), "vendor": "WestTex Chemicals"},
            {"desc": "VFD Monitoring & Software", "gl": "605420", "range": (300, 600), "vendor": "Oilfield Tech Solutions"},
            {"desc": "Corporate G&A Overhead", "gl": "822110", "range": (300, 800), "vendor": "Apex Internal"}
        ],
        "Ad_Hoc": [
            {"desc": "High-Volume Water Disposal Fee", "gl": "605520", "range": (4000, 9000), "vendor": "Blue Water Logistics", "prob": 0.90},
            {"desc": "VFD Tuning / Reset", "gl": "605740", "range": (450, 900), "vendor": "Permian Electric", "prob": 0.25},
            {"desc": "Batch Acid Treatment", "gl": "605620", "range": (2500, 5000), "vendor": "WestTex Chemicals", "prob": 0.15}
        ]
    }
}

# ---------------- LIFT ASSIGNMENT ----------------
LIFTS = ["ESP", "Rod Pump", "Gas Lift"]

def get_lift(asset_id):
    # deterministic assignment (same asset_id → same lift always)
    random.seed(asset_id)
    return random.choice(LIFTS)

# ---------------- WORKOVER ----------------
def build_workover(asset_id, date, lift):
    rig_rate = random.uniform(550, 750)

    if lift == "ESP":
        total = (random.uniform(36, 72) * rig_rate) + random.uniform(45000, 70000) + random.uniform(18000, 28000)
        desc = "ESP Workover"
    elif lift == "Rod Pump":
        total = (random.uniform(12, 24) * rig_rate) + random.uniform(2000, 5000)
        desc = "Rod Pump Workover"
    else:
        total = random.uniform(8, 16) * 450
        desc = "Gas Lift Valve Workover"

    return (
        f"APENR-{date[:4]}-{random.randint(10000,99999)}",
        date,
        "Workover",
        "710520",
        desc,
        "High Plains Workover",
        asset_id,
        round(total, 2)
    )

# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("Generating invoices from DB (no lift column)...")

    cur = conn.cursor()

    # ✅ STEP 1: Fetch only what exists
    cur.execute("""
        SELECT asset_id, date, status
        FROM production_history
        ORDER BY asset_id, date
    """)

    rows = cur.fetchall()

    # ✅ STEP 2: Group by asset_id
    grouped = defaultdict(list)
    for asset_id, date, status in rows:
        grouped[asset_id].append((date, status))

    buffer = io.StringIO()

    # ✅ STEP 3: Generate invoices
    for asset_id, records in grouped.items():
        lift = get_lift(asset_id)   # 🔥 assign lift here
        fail_start = None

        for date, status in records:
            date = str(date)

            # Monthly invoices
            if date.endswith("-28"):
                for exp in EXPENSE_LIBRARY[lift]["Monthly"]:
                    buffer.write(
                        f"APENR-{date[:4]}-{random.randint(10000,99999)},"
                        f"{date},Monthly LOE,{exp['gl']},{exp['desc']},"
                        f"{exp['vendor']},{asset_id},{round(random.uniform(*exp['range']),2)}\n"
                    )

                for exp in EXPENSE_LIBRARY[lift]["Ad_Hoc"]:
                    if random.random() < exp["prob"]:
                        buffer.write(
                            f"APENR-{date[:4]}-{random.randint(10000,99999)},"
                            f"{date},Ad-Hoc LOE,{exp['gl']},{exp['desc']},"
                            f"{exp['vendor']},{asset_id},{round(random.uniform(*exp['range']),2)}\n"
                        )

            # Workover logic
            if status == "FAILURE" and fail_start is None:
                fail_start = date
            elif status == "OK" and fail_start:
                row = build_workover(asset_id, date, lift)
                buffer.write(",".join(map(str, row)) + "\n")
                fail_start = None

    buffer.seek(0)

    # ✅ STEP 4: Insert into DB
    cur.copy_from(
        buffer,
        'invoices',
        sep=',',
        columns=(
            'invoice_id',
            'invoice_date',
            'category',
            'gl_code',
            'service_description',
            'vendor',
            'asset_id',
            'total_usd'
        )
    )

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Invoices generated & inserted successfully!")