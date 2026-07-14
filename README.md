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
| `src/gateway/` | Validates proposals against allow-list, writes permitted to `agent-actions` |
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

Start consumers first, then produce. Full runbook in `DEMO.md`.

```bash
python3 -m src.agent.agent          # tab 1: agent
python3 -m src.governance.consumer  # tab 2: governance
python3 -m src.producer.producer    # tab 3: producer (~40 events, then Ctrl-C)
```

The agent prints a mix of `no_action` (ordinary amounts) and `flag_for_review`
(large or anomalous transactions).

Verify the audit chain:

```bash
python3 -m src.common.verify_audit   # expect: [OK] Chain intact
```

Inspect results:

```bash
rpk topic consume agent-proposals --num 5 --offset start     # agent decisions
rpk topic consume agent-safe-events --num 3 --offset start   # PII is masked
```

Reset for a clean demo run:

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

## Known limits

Hash-chaining gives tamper-*evidence*, not tamper-*proofing* (true proofing needs
external anchoring / WORM / signing). The audit chain assumes a single writer at a
time; concurrent writers can fork it. The LLM is the throughput bottleneck — one
API call per event — so at scale you would batch, cache, and fan out across
partitions with a consumer group.