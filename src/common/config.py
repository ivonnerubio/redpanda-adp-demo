# src/common/config.py
import os
from dotenv import load_dotenv

load_dotenv()

def load_kafka_config() -> dict:
    conf = {
        "bootstrap.servers": os.environ["REDPANDA_BOOTSTRAP_SERVERS"],
    }
    # Redpanda Cloud trial uses SASL/SSL; local rpk usually needs none of this.
    if os.getenv("REDPANDA_SASL_USERNAME"):
        conf.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": os.getenv("REDPANDA_SASL_MECHANISM", "SCRAM-SHA-256"),
            "sasl.username": os.environ["REDPANDA_SASL_USERNAME"],
            "sasl.password": os.environ["REDPANDA_SASL_PASSWORD"],
        })
    return conf 