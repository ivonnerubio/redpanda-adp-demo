# src/producer/producer.py
import json
import time
import signal
import sys
from confluent_kafka import Producer

from src.common.config import load_kafka_config  # your .env-backed config
from src.producer.generate_transaction import generate_transaction

RATE_PER_SEC = 5          # N events/sec — tune as needed
TOPIC = "raw-events"


def build_producer() -> Producer:
    # load_kafka_config() should return a dict with bootstrap.servers,
    # security.protocol, sasl.* etc. pulled from your .env — never hardcode creds.
    conf = load_kafka_config()
    return Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY FAILED] {err}", file=sys.stderr)
    else:
        print(f"[OK] {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def main():
    producer = build_producer()
    running = True

    def shutdown(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    interval = 1.0 / RATE_PER_SEC
    sent = 0

    print(f"Producing ~{RATE_PER_SEC}/sec to '{TOPIC}'. Ctrl-C to stop.")
    try:
        while running:
            event = generate_transaction()

            producer.produce(
                topic=TOPIC,
                key=event["transaction_id"],      # keying → same txn to same partition
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)   # serve delivery callbacks without blocking
            sent += 1

            if sent % 20 == 0:
                print(f"...{sent} sent")

            time.sleep(interval)
    finally:
        print("Flushing...")
        producer.flush(10)
        print(f"Done. Total sent: {sent}")


if __name__ == "__main__":
    main()