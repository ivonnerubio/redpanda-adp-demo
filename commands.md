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






# FINAL

# Demo

## Setup (first time only)
```bash
for t in raw-events agent-safe-events agent-proposals audit-log; do rpk topic create $t -p 1 -r 3; done
```

## Reset (clean slate)
```bash
rpk topic delete raw-events agent-safe-events agent-proposals audit-log
for t in raw-events agent-safe-events agent-proposals audit-log; do rpk topic create $t -p 1 -r 3; done
```

## Run (3 tabs, in order)
```bash
python3 -m src.agent.agent          # tab 1: agent
python3 -m src.governance.consumer  # tab 2: governance
python3 -m src.producer.producer    # tab 3: producer (~40 events, then Ctrl-C)
```

## Verify
```bash
python3 -m src.common.verify_audit                          # audit chain intact
rpk topic consume agent-proposals --num 5 --offset start    # agent decisions
rpk topic consume agent-safe-events --num 3 --offset start  # PII is masked
```




NEW FINAL

python -m src.producer.producer      # generate transactions
python -m src.governance.consumer.    # redact + audit
python -m src.agent.agent             # propose actions
python -m src.gateway.gateway         # rule on proposals