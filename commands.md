commands


rpk group seek governance-layer --to start



python -m src.governance.consumer
rpk topic consume agent-safe-events --num 2


create
rpk topic create audit-log

# SSAudit test
# Terminal 1
rpk topic consume audit-log --offset start

# Terminal 2
python -m src.producer.producer

# Terminal 3
python -m src.governance.consumer




For the demo — the money shot: show it passing, then manually tamper with one record and show it catching the break. Easiest way to tamper live: temporarily edit the verifier to alter a field after reading, e.g. right after the poll loop add:
python    if records:


records[5]["policy_version"] = "9.9.9"   # simulate tampering



# Commands

## Setup
rpk topic create audit-log

## Run pipeline (3 terminals)
rpk topic consume audit-log --offset start   # T1: watch audit
python -m src.producer.producer              # T2: produce
python -m src.governance.consumer            # T3: redact + audit

## Verify
python -m src.common.verify_audit            # -> [OK] Chain intact.

## Tamper demo
# Add after consumer.close(): records[5]["policy_version"] = "9.9.9"
python -m src.common.verify_audit            # -> [FAIL] Record 5 TAMPERED
# Delete line, re-run -> [OK]





# Verify audit agent
python3 -m src.agent.agent
python3 -m src.common.verify_audit