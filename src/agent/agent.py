import json, os, time
from confluent_kafka import Consumer, Producer
from anthropic import Anthropic
from src.common.config import load_kafka_config
from src.common.audit import append_audit

ALLOWED_ACTIONS = ["flag_for_review", "freeze_account", "no_action"]

client = Anthropic(
    api_key=os.environ["ANTHROPIC_API_KEY"],
    timeout=30.0,
    max_retries=2,
)

PROMPT = """You are a fraud-detection analyst reviewing a single card transaction.
The personally-identifying fields have already been redacted; you only see what
you are cleared to see. Decide whether this transaction is suspicious.

Transaction:
{tx}

Respond with ONLY a JSON object, no other text, in exactly this shape:
{{"suspicious": true or false, "reason": "one short sentence", "proposed_action": "one of: flag_for_review, freeze_account, no_action"}}

Guidance: flag_for_review for anything anomalous (high amount, risky category,
geo mismatch). Reserve freeze_account for clear, severe fraud signals. Use
no_action for ordinary transactions."""


def classify(safe_event: dict) -> dict:
    """Call the LLM, parse defensively. Always returns a valid decision dict."""
    tx = json.dumps(safe_event, indent=2)
    try:
        resp = client.messages.create(
            model="claude-sonnet-5",
            max_tokens=200,
            messages=[{"role": "user", "content": PROMPT.format(tx=tx)}],
        )
        text = resp.content[0].text.strip()
        # strip code fences if the model added them
        if text.startswith("```"):
            text = text.split("```")[1].replace("json", "", 1).strip()
        parsed = json.loads(text)
    except Exception as e:
        print(f"[agent] classify error, failing closed to flag_for_review: {type(e).__name__}: {e}", flush=True)        # fail closed: if the LLM misbehaves, flag for a human rather than drop
        return {
            "suspicious": True,
            "reason": f"llm_parse_error: {type(e).__name__}",
            "proposed_action": "flag_for_review",
            "llm_ok": False,
        }

    action = parsed.get("proposed_action")
    if action not in ALLOWED_ACTIONS:
        # model proposed something outside the vocabulary -> fail closed
        parsed["proposed_action"] = "flag_for_review"
        parsed["reason"] = f"coerced_invalid_action:{action}"
    parsed["llm_ok"] = True
    return parsed


def build_clients(conf):
    consumer = Consumer({
        **conf,
        "group.id": "fraud-agent",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer(conf)
    return consumer, producer


def run(conf):
    consumer, producer = build_clients(conf)
    consumer.subscribe(["agent-safe-events"])
    print("fraud agent started, waiting for governed events...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consumer error: {msg.error()}")
                continue

            try:
                safe_event = json.loads(msg.value())
            except json.JSONDecodeError:
                print("skipping malformed safe-event")
                consumer.commit(msg)
                continue

            decision = classify(safe_event)

            proposal = {
                "transaction_id": safe_event.get("transaction_id"),
                "proposed_action": decision["proposed_action"],
                "suspicious": decision.get("suspicious"),
                "reason": decision.get("reason"),
            }

            # emit a PROPOSED action, not a direct one. The gateway decides.
            producer.produce(
                "agent-proposals",
                key=msg.key(),
                value=json.dumps(proposal).encode(),
            )
            producer.flush()

            append_audit({
                "event_id": safe_event.get("transaction_id"),
                "stage": "agent",
                "proposed_action": decision["proposed_action"],
                "suspicious": decision.get("suspicious"),
                "reason": decision.get("reason"),
                "llm_ok": decision.get("llm_ok"),
            })

            print(f"{safe_event.get('transaction_id')}: "
                  f"{decision['proposed_action']} ({decision.get('reason')})")

            consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    run(load_kafka_config())