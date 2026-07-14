import json, os
import yaml
from confluent_kafka import Consumer, Producer
from src.common.config import load_kafka_config
from src.common.audit import append_audit

POLICY_PATH = os.getenv("ACTION_POLICY_PATH", "config/action-policy.yaml")


def load_policy(path: str = POLICY_PATH) -> dict:
    """Read the declarative action policy once, at startup.
    All rulings live in this file, not in code."""
    with open(path) as f:
        return yaml.safe_load(f)


def rule_on(proposed_action: str, policy: dict) -> str:
    """Pure function: given a proposed action, return the ruling.
    No I/O, no side effects -- just look it up in the policy.

    Fail-closed: an action the policy does not list is denied. The gateway
    does not trust upstream to only send known actions; it re-checks every
    time. Unknown capability == unsafe."""
    actions = policy.get("actions", {})
    default = policy.get("defaults", {}).get("unlisted_action_ruling", "deny")
    entry = actions.get(proposed_action)
    if entry is None:
        return default
    return entry.get("ruling", default)


def build_clients(conf):
    consumer = Consumer({
        **conf,
        "group.id": "action-gateway",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer(conf)
    return consumer, producer


def run(conf, policy):
    consumer, producer = build_clients(conf)
    consumer.subscribe(["agent-proposals"])
    output_topic = policy["execution"]["output_topic"]
    policy_version = policy.get("policy_version")
    print("action gateway started, waiting for proposals...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            try:
                proposal = json.loads(msg.value())
            except json.JSONDecodeError:
                print("skipping malformed proposal")
                consumer.commit(msg)
                continue

            proposed_action = proposal.get("proposed_action")
            ruling = rule_on(proposed_action, policy)
            executed = ruling == "allow"

            # Execute ONLY allowed actions: write them to agent-actions.
            # escalate/deny are recorded but never executed.
            if executed:
                action = {
                    "transaction_id": proposal.get("transaction_id"),
                    "action": proposed_action,
                    "reason": proposal.get("reason"),
                }
                producer.produce(
                    output_topic,
                    key=msg.key(),
                    value=json.dumps(action).encode(),
                )
                producer.flush()

            # Log EVERY ruling -- allow, escalate, and deny alike.
            append_audit({
                "event_id": proposal.get("transaction_id"),
                "stage": "gateway",
                "policy_version": policy_version,
                "proposed_action": proposed_action,
                "ruling": ruling,
                "executed": executed,
            })

            print(f"{proposal.get('transaction_id')}: "
                  f"{proposed_action} -> {ruling.upper()}"
                  f"{' (executed)' if executed else ''}")

            consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    run(load_kafka_config(), load_policy())