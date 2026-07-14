# Design

## Thesis: governance outside the agent's data path

The whole system is built around one rule: the agent never touches raw data and
never executes its own actions. Governance is enforced *around* the agent, not
inside it.

Two enforcement points:

1. **Between the agent and the data** — the governance layer consumes raw events,
   redacts PII, and only the sanitized result reaches the agent. The agent's code
   has no path to `raw-events`; it subscribes solely to `agent-safe-events`. Even
   a bug or a prompt-injection in the agent cannot exfiltrate PII it was never
   given.

2. **Between the agent and the world** — the agent emits a *proposed* action to
   `agent-proposals`. It does not act. The action gateway validates each proposal
   against an allow-list and executes only permitted actions. The agent proposes;
   the gateway disposes.

This is deliberately the same shape as the product thesis: control what the agent
sees, limit what it can do, record everything it touches.

## Why declarative policy

Redaction and action rules live in `config/*.yaml`, not in code. Changing what the
agent is cleared to see is a config edit plus a policy-version bump — not an agent
redeploy. In a regulated setting, the person who sets data-clearance policy is not
the person who ships the agent, and this separation reflects that.

The redaction policy fails closed: any field not explicitly listed is dropped, so
schema drift (a new PII field appearing upstream) cannot silently leak to the
agent.

## Why the audit log is hash-chained

Every redaction decision and every agent decision is appended to `audit-log`. Each
record carries a SHA-256 hash of its own contents plus the previous record's hash
(genesis = 64 zeros). Any edit to a past record changes its hash and breaks the
link, which `verify_audit.py` detects.

The chain's previous-hash is read from the topic tail on process startup, not held
only in memory. This means governance and the agent — separate processes — append
to one continuous chain rather than each starting a fresh chain from genesis.

The audit log deliberately records *what was redacted* (field names, strategies,
policy version) but never the raw values, so it cannot become a side-channel that
leaks the very PII it exists to protect.

# Design

## Thesis: governance outside the agent's data path

The whole system is built around one rule: the agent never touches raw data and
never executes its own actions. Governance is enforced *around* the agent, not
inside it.

Two enforcement points:

1. **Between the agent and the data** — the governance layer consumes raw events,
   redacts PII, and only the sanitized result reaches the agent. The agent's code
   has no path to `raw-events`; it subscribes solely to `agent-safe-events`. Even
   a bug or a prompt-injection in the agent cannot exfiltrate PII it was never
   given.

2. **Between the agent and the world** — the agent emits a *proposed* action to
   `agent-proposals`. It does not act. The action gateway validates each proposal
   against an allow-list and executes only permitted actions. The agent proposes;
   the gateway disposes.

This is deliberately the same shape as the product thesis: control what the agent
sees, limit what it can do, record everything it touches.

## Why declarative policy

Redaction and action rules live in `config/*.yaml`, not in code. Changing what the
agent is cleared to see is a config edit plus a policy-version bump — not an agent
redeploy. In a regulated setting, the person who sets data-clearance policy is not
the person who ships the agent, and this separation reflects that.

The redaction policy fails closed: any field not explicitly listed is dropped, so
schema drift (a new PII field appearing upstream) cannot silently leak to the
agent.

## Why the audit log is hash-chained

Every redaction decision and every agent decision is appended to `audit-log`. Each
record carries a SHA-256 hash of its own contents plus the previous record's hash
(genesis = 64 zeros). Any edit to a past record changes its hash and breaks the
link, which `verify_audit.py` detects.

The chain's previous-hash is read from the topic tail on process startup, not held
only in memory. This means governance and the agent — separate processes — append
to one continuous chain rather than each starting a fresh chain from genesis.

The audit log deliberately records *what was redacted* (field names, strategies,
policy version) but never the raw values, so it cannot become a side-channel that
leaks the very PII it exists to protect.

## Reliability: treating the LLM as untrusted

LLMs produce malformed output in production. The agent parses the model's response
defensively and fails closed:

- Unparseable output → `flag_for_review` (escalate to a human), not a crash.
- A proposed action outside the allow-list → coerced to `flag_for_review`.
- The API client has a timeout and bounded retries, so a stalled call cannot hang
  the consumer indefinitely.

The high-risk action (`freeze_account`) is never executed by the agent; it is
denied/escalated at the gateway, keeping a human in the loop for irreversible acts.

## Tradeoffs made for the demo

- **Tamper-evidence, not tamper-proofing.** Hash-chaining detects edits; it does
  not prevent them. True tamper-proofing needs external anchoring, WORM storage,
  or signing.
- **Single audit writer assumed.** The tail-read chain is correct for one writer
  at a time. Concurrent writers can read the same tail and fork the chain; the
  real fix is a single audit-writer service or an externally ordered store.
- **One LLM call per event.** Simple and clear, but the LLM is the throughput
  bottleneck. See PRODUCTIZE.md.
- **Single partition per topic.** Fine for the demo; real scale uses multiple
  partitions and consumer groups.

  
## Reliability: treating the LLM as untrusted

LLMs produce malformed output in production. The agent parses the model's response
defensively and fails closed:

- Unparseable output → `flag_for_review` (escalate to a human), not a crash.
- A proposed action outside the allow-list → coerced to `flag_for_review`.
- The API client has a timeout and bounded retries, so a stalled call cannot hang
  the consumer indefinitely.

The high-risk action (`freeze_account`) is never executed by the agent; it is
denied/escalated at the gateway, keeping a human in the loop for irreversible acts.

## Tradeoffs made for the demo

- **Tamper-evidence, not tamper-proofing.** Hash-chaining detects edits; it does
  not prevent them. True tamper-proofing needs external anchoring, WORM storage,
  or signing.
- **Single audit writer assumed.** The tail-read chain is correct for one writer
  at a time. Concurrent writers can read the same tail and fork the chain; the
  real fix is a single audit-writer service or an externally ordered store.
- **One LLM call per event.** Simple and clear, but the LLM is the throughput
  bottleneck. See PRODUCTIZE.md.
- **Single partition per topic.** Fine for the demo; real scale uses multiple
  partitions and consumer groups.