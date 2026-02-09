import time, os, csv, json
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_data_raw")
CSV_PATH = os.getenv("CSV_PATH", "/data/weather_data.csv")

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
})

with open(CSV_PATH, newline="", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        event = {
            "location": row["Location"],
            "timestamp": row["Date_Time"],
            "temperature_c": float(row["Temperature_C"]),
            "humidity_pct": float(row["Humidity_pct"]),
            "precipitation_mm": float(row["Precipitation_mm"]),
            "wind_speed_kmh": float(row["Wind_Speed_kmh"]),
        }

        producer.produce(topic = TOPIC, 
                         value = json.dumps(event).encode("utf-8"))
        producer.poll(0)

producer.flush()
print("done")
