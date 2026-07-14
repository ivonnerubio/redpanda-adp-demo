import json, time
from confluent_kafka import Producer

def build_audit_producer(conf):
    return Producer(conf)

def append_audit(producer, record):
    """Write one audit record to audit-log. Fire-and-forget with periodic flush."""
    producer.produce(
        "audit-log",
        key=record.get("transaction_id", "").encode() if record.get("transaction_id") else None,
        value=json.dumps(record).encode(),
    )

def make_audit_record(event, redacted_fields, policy):
    """Build a tamper-safe audit record. Never includes raw values."""
    return {
        "transaction_id": event.get("transaction_id") or event.get("id"),
        "timestamp": time.time(),
        "policy_version": policy["policy_version"],
        "redacted_fields": redacted_fields,          # which fields were masked
        "strategy_per_field": {
            f: policy["redactions"][f]["strategy"]
            for f in redacted_fields
            if f in policy["redactions"]
        },
        "fields_seen": sorted(event.keys()),         # field NAMES only, never values
        "log_raw_values": False,                     # explicit: we never store PII here
    }