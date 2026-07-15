#!/usr/bin/env bash
#
# run.sh -- end-to-end demo harness for the fraud-monitoring pipeline.
#
# Runs each component in sequence (never simultaneously) so the append-only
# audit hash-chain has exactly one writer at a time. Each consumer runs in
# DEMO_DRAIN mode: it processes whatever is waiting in its topic, then exits.
#
# Pipeline:
#   producer   -> raw-events
#   governance -> agent-safe-events   (redacts PII, logs to audit-log)
#   agent      -> agent-proposals     (LLM classifies, proposes action)
#   gateway    -> agent-actions       (allow/deny/escalate, logs every ruling)
#   verify_audit                      (confirms the hash-chain is intact)

set -euo pipefail

echo "=============================================="
echo " Fraud-monitoring pipeline -- demo run"
echo "=============================================="

# --- Step 0: reset consumer groups to a clean state -----------------
# Move each group's committed offset past existing messages so this demo
# starts fresh and does not replay old test data (which would also spend
# API credits in the agent). Topics themselves are left intact.
echo
echo ">>> Step 0: resetting consumer groups to end"
rpk group seek governance-layer --to end --topics raw-events
rpk group seek fraud-agent      --to end --topics agent-safe-events
rpk group seek action-gateway   --to end --topics agent-proposals

# --- Step 1: produce transactions -----------------------------------
echo
echo ">>> Step 1: producing transactions to raw-events"
python3 -m src.producer.producer

# --- Step 2: governance redacts PII ---------------------------------
echo
echo ">>> Step 2: governance -- redacting PII into agent-safe-events"
DEMO_DRAIN=1 python3 -m src.governance.consumer

# --- Step 3: agent classifies ---------------------------------------
echo
echo ">>> Step 3: agent -- classifying and proposing actions"
DEMO_DRAIN=1 python3 -m src.agent.agent

# --- Step 4: gateway rules on proposals -----------------------------
echo
echo ">>> Step 4: gateway -- ruling on proposed actions"
DEMO_DRAIN=1 python3 -m src.gateway.gateway

# --- Step 5: verify the audit hash-chain ----------------------------
echo
echo ">>> Step 5: verifying audit-log hash-chain"
python3 -m src.common.verify_audit

echo
echo "=============================================="
echo " Demo run complete."
echo "=============================================="