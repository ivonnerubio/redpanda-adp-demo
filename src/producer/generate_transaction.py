from faker import Faker
import uuid, random
from datetime import datetime, timezone

fake = Faker()

def _realistic_amount():
    # Mimic real card spend: mostly small, a few moderate, rare large.
    roll = random.random()
    if roll < 0.75:
        return round(random.uniform(3.0, 80.0), 2)      # everyday: coffee, lunch, gas
    elif roll < 0.95:
        return round(random.uniform(80.0, 600.0), 2)    # moderate: groceries, clothing
    else:
        return round(random.uniform(600.0, 6000.0), 2)  # large: electronics, travel, rare

def generate_transaction(profile: str = "random"):
    """Generate one transaction.

    profile:
      "random" -> normal random spend (default)
      "fraud"  -> obviously-fraudulent: large amount, high-risk signals.
                  Seeded so the agent reliably proposes freeze_account,
                  which the gateway escalates -- guarantees a live ESCALATE.
      "clean"  -> obviously-ordinary: small everyday spend, no red flags.
    """
    tx = {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": _realistic_amount(),
        "merchant": fake.company(),
        "card_number": fake.credit_card_number(),   # PAN -- redacted in Phase 2
        "holder_name": fake.name(),                  # PII -- redacted
        "ssn": fake.ssn(),                           # PII -- redacted
        "geo": {"city": fake.city(), "country": fake.country_code()},
    }

    if profile == "fraud":
        # Stack severe fraud signals so classification is unambiguous.
        tx["amount"] = round(random.uniform(8000.0, 12000.0), 2)  # far above normal
        tx["merchant"] = "Offshore Crypto Exchange Ltd"           # high-risk category
        tx["geo"] = {"city": "Unknown", "country": "KP"}          # sanctioned/mismatch
    elif profile == "clean":
        # Unambiguously ordinary everyday spend.
        tx["amount"] = round(random.uniform(4.0, 25.0), 2)
        tx["merchant"] = "Corner Coffee House"
        tx["geo"] = {"city": "Austin", "country": "US"}

    return tx

# scratch test -- run from repo root: python -m src.producer.generate_transaction
if __name__ == "__main__":
    from pprint import pprint
    pprint(generate_transaction("fraud"))
    pprint(generate_transaction("clean"))
    pprint(generate_transaction())