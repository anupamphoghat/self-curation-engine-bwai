#!/usr/bin/env bash
# =============================================================================
# scripts/reset_demo.sh — Pre-Demo Environment Reset
# =============================================================================
# Run this 5–10 minutes before your talk to get a clean slate.
# It clears:
#   - BigQuery transactions table (keeps schema, deletes rows)
#   - Firestore mapping_memory collection (clears the cache)
#   - Firestore pending_mappings collection (clears any stale HITL state)
#
# Then optionally re-seeds BigQuery with historical data so the table
# doesn't look empty when you open it at the start of the demo.
#
# Usage:
#   chmod +x scripts/reset_demo.sh
#   ./scripts/reset_demo.sh
# =============================================================================
set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[RESET]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
BQ_DATASET="${BQ_DATASET:-retail_curated}"
BQ_TABLE="${BQ_TABLE:-transactions}"

echo ""
echo "============================================"
echo "  Self-Curation Engine — Demo Reset"
echo "  Project: $PROJECT_ID"
echo "============================================"
echo ""

# ── 1. Clear BigQuery table ───────────────────────────────────────────────────
info "Clearing BigQuery table: ${BQ_DATASET}.${BQ_TABLE}"
bq query \
    --use_legacy_sql=false \
    --project_id="$PROJECT_ID" \
    --location="${REGION:-us-central1}" \
    "DELETE FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\` WHERE TRUE"
echo "  ✓ Table cleared"

# ── 2. Re-seed BigQuery with historical data ──────────────────────────────────
info "Seeding BigQuery with historical data (pre-demo baseline)..."
bq load \
    --source_format=NEWLINE_DELIMITED_JSON \
    --project_id="$PROJECT_ID" \
    --location="${REGION:-us-central1}" \
    "${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}" \
    ./data/seed_data.jsonl
echo "  ✓ Seed data loaded (4 rows)"

# ── 3. Clear Firestore collections via Python ─────────────────────────────────
info "Clearing Firestore mapping_memory and pending_mappings collections..."
python3 - <<'PYEOF'
import os, sys
sys.path.insert(0, '.')
import config
from google.cloud import firestore

db = firestore.Client(project=config.PROJECT_ID)

def delete_collection(name):
    docs = db.collection(name).stream()
    deleted = 0
    for doc in docs:
        doc.reference.delete()
        deleted += 1
    print(f"  ✓ Deleted {deleted} docs from '{name}'")

delete_collection(config.FS_MEMORY_COLLECTION)
delete_collection(config.FS_PENDING_COLLECTION)
PYEOF

# ── 4. Firestore is intentionally left EMPTY after clearing ──────────────────
# DO NOT seed Firestore here. The demo flow relies on:
#   Act 1: CACHE MISS → Gemini call → AUTO_APPROVE → mapping saved to Firestore
#   Act 3: CACHE HIT  ← uses the mapping Act 1 wrote to Firestore
#
# Pre-seeding here would cause Act 1 to silently cache-hit and skip Gemini,
# breaking the demonstration of the first-encounter flow.
#
# If you need to manually seed Firestore (e.g. as a disaster-recovery fallback
# if Act 1 fails mid-demo), run separately:
#   python3 scripts/seed_firestore.py
info "Firestore cleared — leaving empty so Act 1 demonstrates a fresh CACHE MISS."
echo "  ✓ Firestore ready (empty — Act 1 will populate it)"

# ── 5. Warm Cloud Run ─────────────────────────────────────────────────────────
# Use a GET request — a POST with bare JSON triggers CloudEvent parsing and
# generates "Failed to find specversion" errors in the demo logs.
# A GET returns 405 Method Not Allowed but warms the container cleanly.
info "Pinging Cloud Run to eliminate cold-start risk..."
SERVICE_URL=$(gcloud run services describe self-curation-engine \
    --region "${REGION:-us-central1}" \
    --format 'value(status.url)' \
    --project "$PROJECT_ID" 2>/dev/null || echo "")

if [[ -n "$SERVICE_URL" ]]; then
    curl -s -o /dev/null -w "  HTTP %{http_code} — container warm\n" \
        -X GET \
        "$SERVICE_URL" || warn "Ping returned error — container is warming up, wait 5 s"
else
    warn "Could not resolve Cloud Run URL — ping skipped"
fi

echo ""
echo "============================================"
echo -e "  ${GREEN}RESET COMPLETE — You're demo-ready!${NC}"
echo ""
echo "  BigQuery:  4 seed rows loaded"
echo "  Firestore: EMPTY (Act 1 will populate mapping_memory)"
echo "  Cloud Run: container warm"
echo ""
echo "  DEMO FILE ORDER — run in this exact order:"
echo "  Act 1: data/legacy_pos_export_v1.csv  → CACHE MISS → Gemini → AUTO_APPROVE"
echo "  Act 2: data/ambiguous_schema.csv      → CACHE MISS → HITL (logs + curl /approve)"
echo "  Act 3: data/legacy_pos_export_v1.csv  → CACHE HIT  (from Act 1)"
echo ""
echo "  IMPORTANT: Act 3 depends on Act 1 completing first."
echo "  If Act 1 fails, run: python3 scripts/seed_firestore.py"
echo "============================================"
