import json, yaml
from confluent_kafka import Consumer, Producer
from src.governance.redactor import apply_redaction
from src.common.config import load_kafka_config
from src.common.audit import append_audit

def load_policy(path="config/redaction-policy.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)

def build_clients(conf):
    consumer = Consumer({
        **conf,
        "group.id": "governance-layer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer(conf)
    return consumer, producer

def run(conf):
    policy = load_policy()
    consumer, producer = build_clients(conf)
    consumer.subscribe(["raw-events"])
    print("governance consumer started, waiting for messages...")  # <-- add

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("...polling, no message")
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value())
            except json.JSONDecodeError:
                print("skipping malformed event")
                consumer.commit(msg)
                continue

            safe_event, redacted = apply_redaction(event, policy)

            producer.produce(
                "agent-safe-events",
                key=msg.key(),
                value=json.dumps(safe_event).encode(),
            )
            producer.flush()
            
            append_audit({
            "event_id": event.get("id"),
            "redacted_fields": redacted,
            "policy_version": policy["policy_version"],
            })

            consumer.commit(msg)
    finally:
        consumer.close()

if __name__ == "__main__":
    from src.common.config import load_kafka_config
    run(load_kafka_config())