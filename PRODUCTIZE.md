# What I'd productize next

This demo is a working slice, not production. If I were turning it into something a
customer team maintains after I leave — or feeding it back as Redpanda product
features — here is what I'd build, roughly in priority order.

## 1. A managed declarative policy engine

The redaction and action policies are the most reusable idea here. Productized,
this is a policy engine that ships with Redpanda: declare field classifications
and strategies (mask, hash, drop, passthrough) in config, and have the governance
consumer generated or configured from it. Customers in regulated industries get
PII/PCI/PHI redaction without writing consumer code.

## 2. Policy versioning and provenance

Each audit record already stamps `policy_version`. The next step is making policy
itself a first-class, versioned, auditable object: who changed the clearance rules,
when, and which events were processed under which version. This is what a compliance
reviewer actually asks for.

## 3. The audit chain as a managed feature

Hash-chained, tamper-evident audit is generically useful for any agent workload,
not just this one. As a managed feature it would include: a single ordered
audit-writer (removing the fork race in this demo), external anchoring or signing
for true tamper-proofing, and a built-in verifier. "Every action your agent took,
provably unaltered" is a strong governance story.

## 4. A redaction library

Extract the redaction strategies into a small, tested, standalone library with a
clear extension point for custom strategies. This is the piece a customer is most
likely to need to modify, so it should be the cleanest to depend on and extend.

## 5. Scale-out for the LLM bottleneck

The agent makes one LLM call per event, which is the throughput ceiling. Production
path: partition the safe-events topic and run the agent as a consumer group so work
fans out; batch multiple transactions per LLM call; cache decisions for repeated
patterns; and make the API calls async so a slow call doesn't stall the consumer.
For air-gapped or data-residency-constrained customers, support a self-hosted model
endpoint behind the same interface.

## 6. Schema-drift handling

The policy fails closed on unlisted fields today, which is safe but blunt. A
productized version would surface *when* drift happens — alert that an unrecognized
field appeared and was dropped — so policy owners can classify it deliberately
rather than silently losing data.