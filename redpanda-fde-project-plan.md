# Redpanda FDE Interview Project — Agentic Data Plane in Miniature

**Goal:** Build a small but *production-shaped* demo that mirrors Redpanda's core thesis — an LLM agent interacting with streaming data, with a **governance layer enforced outside the agent's data path** that controls what the agent sees, limits what it can do, and captures a tamper-proof audit trail. Ship something that actually runs, is documented, and can be handed off. This is the exact skill the FDE role is testing.

**Deadline:** Wednesday (hiring-manager round)
**Owner:** You

---

## 1. Why this project wins

The FDE role is not about demos or advising — it's about **building agents, integrations, and pipelines that work after you leave**. This project is a direct proof of that:

- **Mirrors their product positioning verbatim.** Redpanda "enforces governance entirely outside the agent's data path, controlling what agents see, limiting what they do, and capturing a tamper-proof record of everything they touch." You're building a working micro-version of that.
- **Demonstrates the exact FDE skills listed:** production-quality agents, integrations, and data pipelines on Redpanda; agent + LLM API experience; the judgment to know reliable vs. brittle.
- **Hits their bonus criteria:** pick a regulated-industry domain (financial services / healthcare) to show you think about compliance and air-gapped-style constraints.
- **Shows handoff mindset:** clean docs, README, "how to run," "what I'd productize next." FDEs are judged on *drive toward exit*, not cleverness.

The hiring manager cares about **judgment and production-thinking** as much as the code. Build a small working slice, then be ready to talk tradeoffs and scale.

---

## 2. Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │                REDPANDA                      │
                    │                                              │
   Producer ───────▶│  topic: raw-events                          │
   (synthetic       │        │                                    │
    data gen)       │        ▼                                    │
                    │  ┌──────────────────┐                       │
                    │  │ GOVERNANCE LAYER │  (consumer + policy)  │
                    │  │  - field redaction                       │
                    │  │  - policy check   │──▶ topic: audit-log   │
                    │  └──────────────────┘   (append-only,       │
                    │        │                 tamper-evident)     │
                    │        ▼                                    │
                    │  topic: agent-safe-events                   │
                    │        │                                    │
                    └────────┼─────────────────────────────────────┘
                             ▼
                    ┌──────────────────┐
                    │      AGENT        │
                    │  - consumes safe  │
                    │    events         │
                    │  - LLM reasoning  │
                    │  - proposes action│
                    └──────────────────┘
                             │
                             ▼  (action request)
                    ┌──────────────────┐
                    │  ACTION GATEWAY   │  outside agent's path
                    │  - allow/deny by  │──▶ topic: audit-log
                    │    policy         │
                    │  - executes only  │
                    │    permitted acts │──▶ topic: agent-actions
                    └──────────────────┘
```

**The key design decision (say this out loud in the interview):** the agent never touches raw data and never executes its own actions directly. Governance sits *between* the agent and the data, and *between* the agent and the world. The agent proposes; the gateway disposes. That's the "outside the data path" principle — the whole point of Redpanda's pitch.

### Components

1. **Producer** — generates synthetic domain events (e.g., financial transactions) and writes to `raw-events`.
2. **Governance layer** — consumes `raw-events`, applies a declarative policy (redact SSN/PAN/PII fields the agent isn't cleared to see), writes the sanitized event to `agent-safe-events`, and logs every decision to `audit-log`.
3. **Agent** — consumes `agent-safe-events`, uses an LLM to reason (e.g., "is this transaction suspicious?"), and emits a *proposed action* rather than acting directly.
4. **Action gateway** — validates the proposed action against an allow-list policy, executes only permitted actions to `agent-actions`, and logs allow/deny to `audit-log`.
5. **Audit log** — an append-only topic; each record chained with a hash of the previous (simple tamper-evidence). This is the "tamper-proof record."

---

## 3. Domain choice

Pick **financial transaction monitoring** (fraud/AML flavor). Reasons:
- Regulated industry → hits their bonus criterion directly.
- Natural PII/PCI fields to redact (card number, SSN, account holder name) → makes the governance layer *obviously* meaningful.
- Clear agent task: flag suspicious transactions → concrete, demoable LLM reasoning.
- Clear action allow-list: `flag_for_review` (allowed), `freeze_account` (requires human, so denied/escalated) → shows "limiting what agents do."

Alternative if you prefer: healthcare (PHI redaction, care-coordination agent) or support tickets (PII redaction, triage agent). Same architecture, swap the schema.

---

## 4. Tech stack

| Layer | Choice | Notes |
|---|---|---|
| Streaming | Redpanda (your trial cluster) | Use Redpanda Cloud trial or `rpk` local |
| Language | **Your strongest** (Python or Go recommended) | FDE role is language-flexible; play to strength. Python = fastest to ship + best LLM SDKs |
| Kafka client | `confluent-kafka` (Python) or `franz-go` (Go) | Redpanda is Kafka-API compatible |
| LLM | Anthropic or OpenAI API | Keep prompt simple + deterministic-ish (low temp) |
| Policy | Plain YAML/JSON config | Declarative = looks production-minded |
| CLI/admin | `rpk` | For creating topics, inspecting messages |


---

## 5. To-Do (ordered, checkbox format)

### Pre Phase
- [X] Streaming: Redpanda
- [X] Language: Python
- [X] Kafka client: confluent-kafka
- [X] LLM: anthropic
- [X] Policy: declarative
- [X] CLI/Admin: rpk

### Phase 0 — Setup (target: done first, ~1 hr)
- [X] Confirm access to Redpanda trial cluster; grab bootstrap servers + credentials
- [X] Install `rpk` and verify connectivity (`rpk cluster info`)
- [X] Create topics: `raw-events`, `agent-safe-events`, `agent-actions`, `audit-log`
- [X] Set up project repo with clean structure (see §7)
- [X] Confirm LLM API key works with a 1-line test call
- [X] Create `.env` / config for cluster + API creds (never hardcode)

### Phase 1 — Data pipeline (target: ~1.5 hrs)
- [X] Write synthetic transaction generator (faker-style: amount, merchant, card number, holder name, SSN, geo, timestamp)
- [X] Producer publishes N events/sec to `raw-events`
- [X] Verify with `rpk topic consume raw-events`

### Phase 2 — Governance layer (target: ~2 hrs) ← **the centerpiece**
- [X] Define policy config: which fields are redacted for the agent (PAN, SSN, full name → masked)
- [X] Consumer reads `raw-events`, applies redaction, produces to `agent-safe-events`
- [X] Every redaction decision logged to `audit-log` (what was seen, what was masked, timestamp, policy version)
- [X] Implement hash-chaining on audit records for tamper-evidence
- [X] Verify: agent-safe-events contains masked data; audit-log is complete

### Phase 3 — Agent (target: ~2 hrs)
- [X] Consumer reads `agent-safe-events`
- [X] LLM prompt: given (masked) transaction, decide if suspicious + propose an action from the allowed vocabulary
- [X] Agent emits a *proposed action* object (not a direct action) to an internal channel/topic
- [X] Make output structured (JSON) and parse defensively — handle malformed LLM output
- [X] Log agent reasoning to `audit-log`

### Phase 4 — Action gateway (target: ~1.5 hrs)
- [X] Define action allow-list policy (`flag_for_review` = allow; `freeze_account` = deny/escalate)
- [X] Gateway validates proposed action → executes permitted, blocks the rest
- [X] Every allow/deny logged to `audit-log`
- [X] Permitted actions written to `agent-actions`

### Phase 5 — Demo harness + observability (target: ~1.5 hrs)
- [X] A single `make demo` / `run.sh` that spins up all components
- [X] A simple console view (or tiny web page) showing: raw event → what agent saw → agent decision → gateway ruling → audit entry
- [X] Seed one obviously-fraudulent and one clean transaction so the demo tells a story live
- [X] Show the audit-log hash-chain verifying (tamper-check command)

### Phase 6 — Handoff artifacts (target: ~1.5 hrs) ← **FDE differentiator**
- [X] `README.md`: what it is, architecture diagram, how to run, config reference
- [X] `DESIGN.md`: the "outside the data path" rationale + tradeoffs
- [X] `PRODUCTIZE.md`: what I'd build next / what should be a Redpanda product feature
- [X] Clean up code, comments, remove dead paths
- [X] Record a 2–3 min screen capture as backup in case live demo has issues

### Phase 7 — Interview prep (target: night before)
- [ ] Rehearse the 5-min walkthrough narrative
- [ ] Prepare answers to "what breaks at scale?" (see §8)
- [ ] Prepare "what I'd do differently with more time"
- [ ] Have the repo pushed + runnable; test on a clean clone

---

## 6. Talking points to weave in (this is what gets you hired)

- **"Governance outside the agent's data path"** — explicitly narrate that the agent never sees raw PII and never executes its own actions. Say the phrase; it's their language.
- **Reliable vs. brittle agents** — call out where you made the LLM output structured + validated it, because you know LLMs produce malformed output in production.
- **Handoff mindset** — point at the README and PRODUCTIZE doc: "this is built so a customer team maintains it after I leave."
- **Regulated-industry awareness** — PII/PCI redaction, tamper-evident audit, human-escalation for high-risk actions.
- **Honesty about scope** — "This is a working slice, not production. Here's exactly what I'd harden next." That candor reads as senior.

---

## 7. Suggested repo structure

```
redpanda-adp-demo/
├── README.md
├── DESIGN.md
├── PRODUCTIZE.md
├── .env.example
├── config/
│   ├── redaction-policy.yaml
│   └── action-policy.yaml
├── src/
│   ├── producer/            # synthetic event generator
│   ├── governance/          # redaction + audit
│   ├── agent/               # LLM reasoning
│   ├── gateway/             # action allow/deny
│   └── common/              # config, audit hash-chain, kafka helpers
├── scripts/
│   ├── create-topics.sh
│   └── run-demo.sh
└── demo/
    └── seed-events.json     # the fraud + clean story events
```

---

## 8. Anticipated hiring-manager questions (prep answers)

1. **"What happens at 100k events/sec?"** — Talk partitioning, consumer groups, the governance layer scaling horizontally, backpressure, and the LLM being the bottleneck (batching, caching, async).
2. **"How is the audit log actually tamper-proof?"** — Be honest: hash-chaining gives tamper-*evidence*; true tamper-proofing needs external anchoring / WORM storage / signing. Shows you know the difference.
3. **"What if the LLM proposes something not in the allow-list?"** — Gateway denies by default; deny-list vs. allow-list; fail-closed design.
4. **"Where would this break in a real customer environment?"** — Schema drift, PII fields you didn't anticipate, policy staleness, LLM latency/cost, air-gapped LLM hosting.
5. **"What would you productize?"** — Declarative policy engine, policy versioning, the audit-chain as a managed feature, a redaction library. This is the "feed learnings back" part of the JD.

---

## 9. Scope discipline (read this when tempted to over-build)

- **Working > ambitious.** A clean, running, documented slice beats a broken ambitious mess. The JD literally values knowing "the difference between a demo and a production deployment."
- If time runs short, cut in this order: fancy UI → multiple domains → extra action types. **Never cut:** the governance layer, the audit log, or the README.
- Two seed events (one fraud, one clean) is enough to tell the whole story.

---

## 10. Minimum viable version (if you only get a few hours)

Producer → governance redaction → agent (anthropic) → audit log. Skip the action gateway if truly crunched, but *mention* it as the next piece. Even the MVP demonstrates the core thesis.
