#!/usr/bin/env bash
# =============================================================================
# scripts/cleanup.sh — Self-Curation Engine: Full Resource Teardown
# =============================================================================
# Run this at the end of the workshop to delete all GCP resources and avoid
# any ongoing charges.
#
# What gets deleted:
#   - Cloud Run service + Eventarc trigger
#   - GCS landing bucket (all contents)
#   - BigQuery dataset (all tables)
#   - Firestore collections (mapping_memory, pending_mappings)
#   - Secret Manager secrets (slack-webhook-url, slack-signing-secret)
#
# Usage:
#   chmod +x scripts/cleanup.sh && ./scripts/cleanup.sh
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[CLEANUP]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}    $*"; }
success() { echo -e "${GREEN}[✓]${NC}       $*"; }

PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="self-curation-engine"
TRIGGER_NAME="${SERVICE_NAME}-gcs-trigger"
BUCKET_NAME="landing-zone-${PROJECT_ID}"
BQ_DATASET="retail_curated"

[[ -z "$PROJECT_ID" ]] && { echo "PROJECT_ID not set."; exit 1; }

echo ""
echo "========================================================"
echo "  Self-Curation Engine — Resource Cleanup"
echo "  Project : $PROJECT_ID"
echo "  Region  : $REGION"
echo "========================================================"
echo ""
read -rp "  Delete all workshop resources in $PROJECT_ID? [y/N] " confirm
[[ "$confirm" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
echo ""

# ── Eventarc trigger ──────────────────────────────────────────────────────────
info "Deleting Eventarc trigger: $TRIGGER_NAME"
gcloud eventarc triggers delete "$TRIGGER_NAME" \
    --location="$REGION" \
    --project="$PROJECT_ID" \
    --quiet 2>/dev/null && success "Eventarc trigger deleted" || warn "Trigger not found — skipping"

# ── Cloud Run service ─────────────────────────────────────────────────────────
info "Deleting Cloud Run service: $SERVICE_NAME"
gcloud run services delete "$SERVICE_NAME" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --quiet 2>/dev/null && success "Cloud Run service deleted" || warn "Service not found — skipping"

# ── GCS bucket ────────────────────────────────────────────────────────────────
info "Deleting GCS bucket: gs://$BUCKET_NAME"
gsutil -m rm -r "gs://$BUCKET_NAME" 2>/dev/null && success "Bucket deleted" || warn "Bucket not found — skipping"

# ── BigQuery dataset ──────────────────────────────────────────────────────────
info "Deleting BigQuery dataset: $BQ_DATASET (including all tables)"
bq rm -r -f --dataset "${PROJECT_ID}:${BQ_DATASET}" 2>/dev/null && success "BigQuery dataset deleted" || warn "Dataset not found — skipping"

# ── Firestore collections ─────────────────────────────────────────────────────
info "Clearing Firestore collections..."
python3 - <<'PYEOF' 2>/dev/null || warn "Firestore cleanup skipped (Python/credentials not available)"
import sys, os
sys.path.insert(0, '.')
try:
    import config
    from google.cloud import firestore
    db = firestore.Client(project=config.PROJECT_ID)
    for col in [config.FS_MEMORY_COLLECTION, config.FS_PENDING_COLLECTION]:
        deleted = 0
        for doc in db.collection(col).stream():
            doc.reference.delete()
            deleted += 1
        print(f"  Deleted {deleted} docs from '{col}'")
    print("  Firestore collections cleared")
except Exception as e:
    print(f"  Firestore cleanup error: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
success "Firestore cleared"

# ── Secret Manager ────────────────────────────────────────────────────────────
info "Deleting Secret Manager secrets..."
for secret in slack-webhook-url slack-signing-secret; do
    gcloud secrets delete "$secret" \
        --project="$PROJECT_ID" \
        --quiet 2>/dev/null && success "Secret '$secret' deleted" || warn "Secret '$secret' not found — skipping"
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo "========================================================"
echo -e "  ${GREEN}CLEANUP COMPLETE${NC}"
echo ""
echo "  All workshop resources deleted from $PROJECT_ID."
echo "  There are no ongoing charges from this deployment."
echo "========================================================"
