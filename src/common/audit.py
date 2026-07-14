#src/common/audit/.py
import json, hashlib, datetime
from confluent_kafka import Producer
from src.common.config import load_kafka_config

_producer = None

def _get_producer():
    global _producer
    if _producer is None:
        _producer = Producer(load_kafka_config())
    return _producer

def append_audit(record: dict):
    # stamp every audit entry with UTC time
    record["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()

    producer = _get_producer()
    producer.produce(
        "audit-log",
        value=json.dumps(record, sort_keys=True).encode(),
    )
    producer.flush()