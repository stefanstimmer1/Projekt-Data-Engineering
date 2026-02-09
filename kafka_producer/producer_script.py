import time
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "kafka:9092"
})

topic = "test"

while True:
    event = str(time.time())

    producer.produce(
        topic = topic,
        value = event.encode("utf-8")
    )
    producer.poll(0)

    print("sent event")
    time.sleep(5)
