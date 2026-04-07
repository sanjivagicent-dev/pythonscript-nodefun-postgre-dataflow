import json
import numpy as np
import random
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
import io

START_DATE = datetime(2014, 1, 1)
END_DATE = datetime(2024, 6, 1)

# ---------------- DB CONNECTION ----------------
conn = psycopg2.connect(
    dbname="oilfield",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5433"
)

# ---------------- BUILD WELLS ----------------
def build_hierarchy():
    wells = []
    lifts = ["Rod Pump", "ESP", "Gas Lift"]
    leases = ["Mustang", "Bronco", "Pinto", "Ranger"]
    
    for i in range(30):
        wells.append({
            "asset_id": f"{leases[i % 4]}-12-{i+1}-H",
            "lease": leases[i % 4],
            "lift_type": random.choice(lifts),
            "spud_date": START_DATE + timedelta(days=random.randint(100, 2000)),
            "base_rate": random.uniform(500, 1200)
        })
    return wells

# ---------------- SIMULATION ----------------
def simulate_physics(well):
    dates = pd.date_range(well["spud_date"], END_DATE, freq="D")
    lift = well["lift_type"]

    t = np.arange(len(dates))
    decline = 0.0006
    theoretical_rates = well["base_rate"] * np.exp(-decline * t)

    variance = 0.02 if lift == "ESP" else (0.035 if lift == "Rod Pump" else 0.05)
    rates = np.clip(theoretical_rates * np.random.normal(1.0, variance, len(dates)), 0, None)

    history_log = []
    status = "OK"
    days_online = 0
    downtime_rem = 0
    failure_note = ""
    failure_start_flag = False   # ✅ FIX

    for i, d in enumerate(dates):
        date_str = d.strftime("%Y-%m-%d")

        if status == "OK":
            days_online += 1
            daily_oil = rates[i]

            motor_temp = (
                170 + (days_online * 0.02) + random.uniform(-2, 2)
                if lift == "ESP"
                else (120 if lift == "Rod Pump" else 100)
            )

            fail_prob = 0.00005
            if lift == "ESP" and days_online > 450:
                fail_prob += 0.003 * ((days_online - 450) / 100)
                if motor_temp > 210:
                    fail_prob *= 2.5
            elif lift == "Rod Pump" and days_online > 300:
                fail_prob += 0.002 * ((days_online - 300) / 100)
            elif lift == "Gas Lift" and days_online > 600:
                fail_prob += 0.001 * ((days_online - 600) / 150)

            if random.random() < fail_prob:
                status = "FAILURE"
                downtime_rem = random.randint(4, 12)
                failure_start_flag = True  # ✅ FIX

                failure_note = (
                    "ESP Motor/Cable Failure" if lift == "ESP"
                    else "Parted Sucker Rods" if lift == "Rod Pump"
                    else "Gas Lift Valve Failure"
                )
                daily_oil = 0

        else:
            daily_oil = 0
            motor_temp = 0
            downtime_rem -= 1

            if downtime_rem <= 0:
                status = "OK"
                days_online = 0

        history_log.append({
            "date": date_str,
            "allocated_oil": round(daily_oil, 2),
            "motor_temp_f": round(motor_temp, 2),
            "status": status,
            "notes": failure_note if failure_start_flag else ""
        })

        failure_start_flag = False  # reset flag

    return history_log

# ---------------- MAIN ----------------
if __name__ == "__main__":
    print("Generating Data...")

    wells = build_hierarchy()
    data = {"assets": {}}

    for w in wells:
        data["assets"][w["asset_id"]] = {
            "lift": w["lift_type"],
            "history": simulate_physics(w)
        }

    print("Inserting into PostgreSQL...")

    cur = conn.cursor()
    buffer = io.StringIO()

    # ✅ FAST INSERT USING COPY
    for asset_id, well in data["assets"].items():
        for h in well["history"]:
            buffer.write(
                f"{asset_id},{h['date']},{h['allocated_oil']},{h['motor_temp_f']},{h['status']},{h['notes']}\n"
            )

    buffer.seek(0)

    cur.copy_from(
    buffer,
    'production_history',
    sep=',',
    columns=('asset_id', 'date', 'allocated_oil', 'motor_temp_f', 'status', 'notes')
)

    conn.commit()
    cur.close()
    conn.close()

    print("✅ Data inserted successfully!")