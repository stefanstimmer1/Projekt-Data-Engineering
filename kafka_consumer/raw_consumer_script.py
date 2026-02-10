import os, json, time
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaException

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "raw_kafka_consumer")
RAW_DIR = os.getenv("RAW_DIR", "/raw") 
ROTATE_MB = 32
FLUSH_EVERY = 500
IDLE_FLUSH_SECS = 10

last_flush = time.time()
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

def utc_date(ts_ms: int):
    # takes ts_ms and returns utc date in this form "%Y-%m-%d"
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")

def file_path(topic: str, date_str: str, seq: int):
    # append-only file per topic/date and rotation if file is to big
    return os.path.join(RAW_DIR, topic, date_str, f"chunk-{seq:04d}.ndjson")

def open_append(path: str):
    # open path
    os.makedirs(os.path.dirname(path), exist_ok=True)
    return open(path, "a", encoding="utf-8", buffering=1)

def current_size_bytes(path: str):
    # get file size
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        return 0

if __name__ == "__main__":
    # check raw dir
    os.makedirs(RAW_DIR, exist_ok=True)

    msg_count = 0
    chunk_seq = 0
    fh = None
    current_path = None
    
    try:
        consumer.subscribe([TOPIC])
        while True:
            msg = consumer.poll(5.0)
            now = time.time()

            # no new messages in topic
            if msg is None:
                # flush and commit if there are still messages
                if msg_count > 0 and (now - last_flush) >= IDLE_FLUSH_SECS:
                    fh.flush()
                    os.fsync(fh.fileno())
                    consumer.commit(asynchronous=False)
                    print(f"Iidle flush: committed msg_count={msg_count}")
                    msg_count = 0
                    last_flush = now
                continue
            if msg.error():
                raise KafkaException(msg.error())

            # kafka metadata
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            _, ts_ms = msg.timestamp()
            if ts_ms is None:
                ts_ms = int(time.time() * 1000)

            date_str = utc_date(ts_ms)

            # open first path
            if current_path is None:
                current_path = file_path(topic, date_str, chunk_seq)
                fh = open_append(current_path)

            # change path if date changes or file size reached ROTATE_MB
            if (date_str not in current_path) or (current_size_bytes(current_path) > ROTATE_MB * 1024 * 1024):
                fh.flush()
                os.fsync(fh.fileno())
                fh.close()
                chunk_seq += 1
                current_path = file_path(topic, date_str, chunk_seq)
                fh = open_append(current_path)

            # save key and value from message
            raw_value = msg.value() or b""
            raw_key = msg.key() or b""
            value = None
            key = None
            try:
                value = json.loads(raw_value.decode("utf-8"))
            except UnicodeDecodeError: 
                value = repr(raw_value)
            try:
                key = raw_key.decode("utf-8")
            except UnicodeDecodeError: 
                key = repr(raw_key)

            # save metadata and message as record
            record = {
                "kafka": {
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "timestamp_ms": ts_ms,
                    "key": key,
                },
                "value": value,
            }

            # append-only write
            fh.write(json.dumps(record, separators=(",", ":")) + "\n")
            msg_count += 1

            # flush, fsync and commit every FLUSH_EVERY record
            if msg_count >= FLUSH_EVERY:
                fh.flush()
                os.fsync(fh.fileno())
                consumer.commit(asynchronous=False)
                print(f"saved {msg_count} msgs (last offset {offset}) -> {current_path}", flush=True)
                msg_count = 0
                last_flush = now
                
    except Exception as e:
        print(e)
    finally:
        # if error occured still flush, sync, close and commit a last time.
        try:
            if fh:
                fh.flush()
                os.fsync(fh.fileno())
                fh.close()
        except Exception as e:
            print(e)
        # final commit nach letztem Write
        try:
            consumer.commit(asynchronous=False)
        except Exception as e:
            print(e)
        consumer.close()
