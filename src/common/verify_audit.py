#src/common/verify_audit.py
import json, hashlib, time
from confluent_kafka import Consumer
from src.common.config import load_kafka_config

def _hash_record(record: dict) -> str:
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()

def verify():
    consumer = Consumer({
        **load_kafka_config(),
        "group.id": f"audit-verifier-{time.time()}",   # fresh group every run -> reads from start
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(["audit-log"])

    records = []
    empty_polls = 0
    while empty_polls < 10:          # allow time for group assignment
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        empty_polls = 0              # reset once we start receiving
        if msg.error():
            continue
        records.append(json.loads(msg.value()))
    consumer.close()

    print(f"Read {len(records)} audit records. Verifying chain...\n")

    prev_hash = "0" * 64
    for i, record in enumerate(records):
        stored_hash = record.pop("hash")            # remove hash before recomputing
        recomputed = _hash_record(record)

        if recomputed != stored_hash:
            print(f"[FAIL] Record {i} TAMPERED: hash mismatch")
            print(f"       event_id: {record.get('event_id')}")
            return
        if record["prev_hash"] != prev_hash:
            print(f"[FAIL] Chain BROKEN at record {i}: prev_hash doesn't match")
            return

        prev_hash = stored_hash

    print(f"[OK] Chain intact. All {len(records)} records verified.")

if __name__ == "__main__":
    verify()