import json, hashlib, datetime, time
from confluent_kafka import Producer, Consumer
from src.common.config import load_kafka_config

_producer = None
_prev_hash = None   # unknown until we sync from the topic


def _get_producer():
    global _producer
    if _producer is None:
        _producer = Producer(load_kafka_config())
    return _producer


def _hash_record(record: dict) -> str:
    encoded = json.dumps(record, sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _load_tail_hash() -> str:
    """Read the current last record on audit-log and return its hash.
    If the topic is empty, return genesis. This makes the chain durable
    across processes: any writer continues from the true tail, not from
    an in-memory reset."""
    consumer = Consumer({
        **load_kafka_config(),
        "group.id": f"audit-tail-{time.time()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(["audit-log"])
    last = None
    empty_polls = 0
    while empty_polls < 10:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        empty_polls = 0
        if msg.error():
            continue
        last = json.loads(msg.value())
    consumer.close()
    if last is None:
        return "0" * 64
    return last["hash"]


def append_audit(record: dict):
    global _prev_hash
    # On first write in this process, sync to the topic's true tail.
    if _prev_hash is None:
        _prev_hash = _load_tail_hash()

    record["timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    record["prev_hash"] = _prev_hash

    this_hash = _hash_record(record)
    record["hash"] = this_hash
    _prev_hash = this_hash

    producer = _get_producer()
    producer.produce(
        "audit-log",
        value=json.dumps(record, sort_keys=True).encode(),
    )
    producer.flush()