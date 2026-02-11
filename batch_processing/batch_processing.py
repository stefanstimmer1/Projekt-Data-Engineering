import os
import json
from collections import defaultdict
import psycopg2
from psycopg2.extras import execute_batch

RAW_ROOT = os.getenv("RAW_ROOT", "/raw")
PG_DSN = os.getenv("PG_DSN", "postgresql://test:test@postgres:5432/weather_data")
TABLE = os.getenv("TABLE", "daily_weather_aggregates")

def iterate_ndjson_files(base_dir: str):
    # reads every file in the raw zone
    for root, _, files in os.walk(base_dir):
        for fn in files:
            if fn.endswith(".ndjson"):
                yield os.path.join(root, fn)

def extract_day(event: dict):
    # extract day from timestamp with form "YYYY-MM-DD HH:MM:SS"
    ts = event.get("timestamp")
    if isinstance(ts, str) and len(ts) >= 10:
        return ts[:10]

if __name__ == "__main__":
    # check path
    if not os.path.isdir(RAW_ROOT):
        raise SystemExit(f"raw zone dir not found: {RAW_ROOT}")

    # aggregation state
    aggregation = defaultdict(lambda: {
        "count": 0,
        "sum_temp": 0.0,
        "sum_humidity": 0.0,
        "sum_precip": 0.0,
        "sum_wind": 0.0,
    })

    # get all ndjson files
    files = list(iterate_ndjson_files(RAW_ROOT))
    print(f"Found {len(files)} ndjson files")

    total_events = 0
    used_events = 0
    temp = None
    hum = None
    precip = None
    wind = None

    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                total_events += 1

                rec = json.loads(line)
                if "value" not in rec:
                    continue

                event = rec["value"]
                location = event.get("location")
                day = extract_day(event)
                if not location or not day:
                    continue

                try:
                    temp = float(event["temperature_c"])
                    hum = float(event["humidity_pct"])
                    precip = float(event["precipitation_mm"])
                    wind = float(event["wind_speed_kmh"])
                except Exception:
                    continue

                s = aggregation[(day, location)]
                s["count"] += 1
                s["sum_temp"] += temp
                s["sum_humidity"] += hum
                s["sum_precip"] += precip
                s["sum_wind"] += wind
                used_events += 1

    print(f"parsed events: total={total_events} used={used_events} groups={len(aggregation)}")

    # prepare data (calculate average)
    rows = []
    for (day, location), s in aggregation.items():
        c = s["count"]
        rows.append((
            day,
            location,
            c,
            s["sum_temp"] / c,
            s["sum_humidity"] / c,
            s["sum_precip"] / c,
            s["sum_wind"] / c,
        ))

    # write to postgres 
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = False

    with conn.cursor() as cur:
        # create table
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
          day date NOT NULL,
          location text NOT NULL,
          count_measurements int NOT NULL,
          avg_temperature_c double precision NOT NULL,
          avg_humidity_pct double precision NOT NULL,
          sum_precipitation_mm double precision NOT NULL,
          avg_wind_speed_kmh double precision NOT NULL,
          PRIMARY KEY (day, location)
        )
        """)
        # insert date to table, overwrite if exists
        execute_batch(cur, f"""
        INSERT INTO {TABLE} (
          day, location, count_measurements,
          avg_temperature_c, avg_humidity_pct, 
          sum_precipitation_mm, avg_wind_speed_kmh
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (day, location) DO UPDATE SET
          count_measurements = EXCLUDED.count_measurements,
          avg_temperature_c = EXCLUDED.avg_temperature_c,
          avg_humidity_pct = EXCLUDED.avg_humidity_pct,
          sum_precipitation_mm = EXCLUDED.sum_precipitation_mm,
          avg_wind_speed_kmh = EXCLUDED.avg_wind_speed_kmh
        """, rows, page_size=500)

    conn.commit()
    conn.close()

    print(f"added {len(rows)} rows into {TABLE} table")

