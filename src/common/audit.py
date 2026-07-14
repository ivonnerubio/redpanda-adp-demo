#src/common/audit.py
import json, hashlib, datetime
from confluent_kafka import Producer
from src.common.config import load_kafka_config

_producer = None
_prev_hash = "0" * 64   # genesis: no previous record yet

def _get_producer():
    global _producer
    if _producer is None:
        _producer = Producer(load_kafka_config())
    return _producer

def _hash_record(record: dict) -> str:
    encoded = json.dumps(record, sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()

def append_audit(record: dict):
    global _prev_hash

    # stamp + link before hashing, so both are part of the fingerprint
    record["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    record["prev_hash"] = _prev_hash

    # fingerprint this record, store it, and chain forward
    this_hash = _hash_record(record)
    record["hash"] = this_hash
    _prev_hash = this_hash

    producer = _get_producer()
    producer.produce(
        "audit-log",
        value=json.dumps(record, sort_keys=True).encode(),
    )
    producer.flush()