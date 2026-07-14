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

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": _realistic_amount(),
        "merchant": fake.company(),
        "card_number": fake.credit_card_number(),   # PAN — redacted in Phase 2
        "holder_name": fake.name(),                  # PII — redacted
        "ssn": fake.ssn(),                           # PII — redacted
        "geo": {"city": fake.city(), "country": fake.country_code()},
    }

# scratch test — run from repo root: python -m src.producer.generate_transaction
if __name__ == "__main__":
    from pprint import pprint
    pprint(generate_transaction())
    pprint(generate_transaction())