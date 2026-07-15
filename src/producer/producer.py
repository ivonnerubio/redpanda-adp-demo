# src/producer/producer.py
import json
import os
import sys
from confluent_kafka import Producer
import random


from src.common.config import load_kafka_config  # your .env-backed config
from src.producer.generate_transaction import generate_transaction

TOPIC = "raw-events"
# Total events to send, then exit. Override with EVENT_COUNT=N.
EVENT_COUNT = int(os.getenv("EVENT_COUNT", "12"))
# How many of the batch are seeded fraud / clean story transactions.
# The rest are random. Override with FRAUD_SEEDS / CLEAN_SEEDS.
FRAUD_SEEDS = int(os.getenv("FRAUD_SEEDS", "3"))
CLEAN_SEEDS = int(os.getenv("CLEAN_SEEDS", "3"))
# SHUFFLE=0 keeps seeds grouped at the front (easy to point at in a demo);
# SHUFFLE=1 (default) mixes them into the batch for a realistic stream.
SHUFFLE = os.getenv("SHUFFLE", "1") == "1"


def build_producer() -> Producer:
    conf = load_kafka_config()
    return Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY FAILED] {err}", file=sys.stderr)
    else:
        print(f"[OK] {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def send(producer, event, label):
    producer.produce(
        topic=TOPIC,
        key=event["transaction_id"],
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)
    print(f"  sent {label:11s}: {event['transaction_id']} "
          f"(${event['amount']}, {event['merchant']})")


def main():
    producer = build_producer()

    fraud = min(FRAUD_SEEDS, EVENT_COUNT)
    clean = min(CLEAN_SEEDS, EVENT_COUNT - fraud)
    random_n = max(0, EVENT_COUNT - fraud - clean)

    batch = (
        [(generate_transaction("fraud"), "FRAUD seed") for _ in range(fraud)]
        + [(generate_transaction("clean"), "CLEAN seed") for _ in range(clean)]
        + [(generate_transaction(), "random") for _ in range(random_n)]
    )

    if SHUFFLE:
        random.shuffle(batch)

    print(f"Producing {len(batch)} events to '{TOPIC}' "
          f"({fraud} fraud, {clean} clean, {random_n} random), then exiting.")
    for event, label in batch:
        send(producer, event, label)

    print("Flushing...")
    producer.flush(10)
    print(f"Done. Total sent: {len(batch)}")


if __name__ == "__main__":
    main()