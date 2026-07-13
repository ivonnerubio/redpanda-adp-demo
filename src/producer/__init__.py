from faker import Faker
import uuid, random
from datetime import datetime, timezone

fake = Faker()

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(1.0, 5000.0), 2),
        "merchant": fake.company(),
        "card_number": fake.credit_card_number(),   # PAN — redacted in Phase 2
        "holder_name": fake.name(),                  # PII — redacted
        "ssn": fake.ssn(),                           # PII — redacted
        "geo": {"city": fake.city(), "country": fake.country_code()},
    }

# scratch test — run from repo root: python -m src.producer.generator
if __name__ == "__main__":
    from pprint import pprint
    pprint(generate_transaction())
    pprint(generate_transaction())