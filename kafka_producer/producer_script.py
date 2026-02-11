import os, csv, json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather_data_raw")
CSV_PATH = os.getenv("CSV_PATH", "/data/weather_data.csv")

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
})

def ensure_topic_exists():
    # ensure topic exists, if not create topic
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})

    metadata = admin.list_topics(timeout=10)

    if TOPIC in metadata.topics:
        print(f"topic \"{TOPIC}\" exists.")
        return

    print(f"topic \"{TOPIC}\" not found.")
    print(f"creating topic \"{TOPIC}\"")
    new_topic = NewTopic(topic=TOPIC)

    futures = admin.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()
            print(f"topic \"{topic}\" created.")
        except Exception as e:
            print(f"failed to create topic {topic}: {e}")
            raise

if __name__ == "__main__":
    ensure_topic_exists()
    try:
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
        print("transfered all data to kafka")
    except Exception as e:
        print(f"failed to transfer data to kafka: {e}")
