# Agentic Data Plane (Redpanda)

A production-shaped demo: an LLM agent reasons over streaming transaction data,
with a governance layer that enforces PII redaction **outside the agent's data
path** and a tamper-evident audit log of every decision.

Domain: financial transaction monitoring (fraud/AML flavor).

## The core idea

The agent never sees raw PII and never executes its own actions. Governance sits
*between* the agent and the data (redaction) and *between* the agent and the world
(the action gateway). The agent proposes; the gateway disposes.

```
producer ──▶ raw-events ──▶ governance ──▶ agent-safe-events ──▶ agent ──▶ agent-proposals ──▶ gateway ──▶ agent-actions
                               │                                   │                              │
                               └──────────────▶ audit-log ◀────────┴──────────────────────────────┘
                                            (hash-chained, tamper-evident)
```

## Components

| Path | Role |
|---|---|
| `src/producer/` | Generates synthetic transactions to `raw-events` |
| `src/governance/` | Redacts PII per policy, writes `agent-safe-events`, logs to `audit-log` |
| `src/agent/` | LLM reasons over safe events, emits proposals to `agent-proposals` |
| `src/gateway/` | Rules on proposals (allow/escalate/deny), writes permitted to `agent-actions`, logs every ruling |
| `src/common/` | Config, audit hash-chain, chain verifier |
| `config/` | `redaction-policy.yaml`, `action-policy.yaml` (declarative) |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # then fill in your real values
```

Fill in `.env` (see `.env.example` for all keys):
- Redpanda Cloud bootstrap + SASL credentials
- `ANTHROPIC_API_KEY`
- `REDACTION_HASH_SALT` — generate one with:

```bash
echo "REDACTION_HASH_SALT=$(openssl rand -hex 16)" >> .env
```

`.env` is gitignored; never commit real credentials.

Create topics (first time only):

```bash
for t in raw-events agent-safe-events agent-proposals audit-log agent-actions; do
  rpk topic create $t -p 1 -r 3
done
```

Note: this cluster (Redpanda Cloud trial) requires replication factor 3.

## Run

### One command (recommended)

`run.sh` runs the whole pipeline end-to-end. It resets the consumer groups to a
clean state, produces a batch of transactions, then runs governance, the agent,
and the gateway **in sequence** — never simultaneously, so the append-only audit
hash-chain has exactly one writer at a time. It ends by verifying the chain.

```bash
./run.sh
```

Each stage runs to completion and exits on its own: the consumers run in
`DEMO_DRAIN` mode (see below), and the producer sends a fixed batch rather than
streaming forever. No Ctrl-C needed.

Tune the batch with environment variables:

```bash
EVENT_COUNT=20 FRAUD_SEEDS=3 CLEAN_SEEDS=3 ./run.sh
```

- `EVENT_COUNT` — total transactions to produce (default 20)
- `FRAUD_SEEDS` — guaranteed obviously-fraudulent transactions (default 3)
- `CLEAN_SEEDS` — guaranteed obviously-clean transactions (default 3)
- the remainder are random spend; `SHUFFLE=0` groups the seeds at the front
  instead of mixing them into the stream

The seeded fraud transactions reliably drive the agent to propose
`freeze_account`, which the gateway **escalates** — so every run shows a live
ESCALATE, not just whatever the random amounts happened to produce.

Note: each event is one paid Claude classification. While iterating on the
harness, use a small `EVENT_COUNT` (e.g. 4); save larger runs for rehearsing the
actual demo.

### Running components by hand

The components can also be run one per terminal, as long-lived services (this is
how they would run in production — they block and wait for messages):

```bash
python3 -m src.governance.consumer  # tab 1: governance
python3 -m src.agent.agent          # tab 2: agent
python3 -m src.gateway.gateway      # tab 3: gateway
python3 -m src.producer.producer    # tab 4: producer (fixed batch, then exits)
```

### Drain mode (DEMO_DRAIN)

Each consumer normally blocks forever waiting for messages. Setting
`DEMO_DRAIN=1` makes it process whatever is currently in its topic and then exit
after a few idle seconds, instead of blocking. This is what lets `run.sh` chain
the stages sequentially. Without the flag, the components behave as ordinary
long-lived services.

```bash
DEMO_DRAIN=1 python3 -m src.agent.agent   # drains agent-safe-events, then exits
```

### Verify and inspect

```bash
python3 -m src.common.verify_audit   # expect: [OK] Chain intact

rpk topic consume agent-proposals --num 5 --offset start     # agent decisions
rpk topic consume agent-actions --num 5 --offset start       # executed (allowed) actions
rpk topic consume agent-safe-events --num 3 --offset start   # PII is masked
```

### Reset for a clean run

`run.sh` already resets on each run by seeking the consumer groups past existing
messages (no credits spent re-reading old events, topics left intact):

```bash
rpk group seek governance-layer --to end --topics raw-events
rpk group seek fraud-agent      --to end --topics agent-safe-events
rpk group seek action-gateway   --to end --topics agent-proposals
```

To wipe everything and start from scratch (rebuilds the audit chain from
genesis):

```bash
rpk topic delete raw-events agent-safe-events agent-proposals audit-log agent-actions
for t in raw-events agent-safe-events agent-proposals audit-log agent-actions; do
  rpk topic create $t -p 1 -r 3
done
```

## Governance details

Redaction is declarative (`config/redaction-policy.yaml`). Strategies:
`mask_all`, `mask_partial`, `hash`, `drop`, `passthrough`. Unlisted fields are
dropped by default (fail-closed against schema drift). Clearance changes are a
config edit + policy-version bump, not an agent redeploy.

The `hash` strategy is salted from `REDACTION_HASH_SALT` (env, not config) so the
agent can correlate by holder without the hash being reversible by guessing.

Every redaction and every agent decision is appended to `audit-log`. Each record
stores a SHA-256 hash of its own contents plus the previous record's hash
(genesis = 64 zeros). Editing any record breaks the chain, which
`verify_audit.py` detects. The chain state is read from the topic tail on startup,
so governance and agent records form one continuous chain across processes.

## The action gateway

Proposals from the agent are ruled on declaratively (`config/action-policy.yaml`):
`no_action` and `flag_for_review` are allowed, `freeze_account` is escalated, and
any action not listed is denied (fail-closed). Only allowed actions are written
to `agent-actions`; escalate and deny are recorded but never executed. Every
ruling — allow, escalate, deny — is appended to the audit log.

The gateway re-checks every proposal even though the agent already coerces
invalid actions upstream. This is deliberate defense in depth: the gateway does
not trust the agent (an LLM can be wrong, manipulated, or bypassed), so the
security boundary holds even if the agent is compromised.

## Reliability

The agent parses LLM output defensively: malformed responses and actions outside
the allow-list fail closed to `flag_for_review` rather than crashing or passing
silently. The API client has a timeout and retries.

## Config reference

| Var | Purpose |
|---|---|
| `REDPANDA_BOOTSTRAP_SERVERS` | Cluster bootstrap |
| `REDPANDA_SASL_USERNAME` / `_PASSWORD` / `_MECHANISM` | SASL auth (Cloud) |
| `ANTHROPIC_API_KEY` | LLM agent |
| `REDACTION_HASH_SALT` | Salt for the `hash` redaction strategy |
| `DEMO_DRAIN` | `1` = consumers drain their topic and exit (demo); unset = run forever (service) |
| `EVENT_COUNT` / `FRAUD_SEEDS` / `CLEAN_SEEDS` / `SHUFFLE` | Producer batch shape |

## Known limits

Hash-chaining gives tamper-*evidence*, not tamper-*proofing* (true proofing needs
external anchoring / WORM / signing). The audit chain assumes a single writer at a
time; concurrent writers can fork it — which is exactly why `run.sh` runs the
stages sequentially. Delivery is at-least-once: the gateway commits after
producing, so a crash in between can re-emit an action (fix: idempotent writes
keyed by transaction_id, or Kafka transactions). The LLM is the throughput
bottleneck — one API call per event — so at scale you would batch, cache, and fan
out across partitions with a consumer group.