#!/usr/bin/env bash
# =============================================================================
# workshop_deploy.sh — Self-Curation Engine: One-Command GCP Deployment
# =============================================================================
# Deploys the complete Self-Curation Engine in your own GCP project.
# Run this script once. At the end it prints all workshop act commands
# pre-filled with your project's bucket and service URL.
#
# What this script does:
#   1. Enables required GCP APIs
#   2. Creates a GCS landing bucket
#   3. Creates the BigQuery dataset + curated table
#   4. Initialises Firestore (Native mode)
#   5. Stores Slack credentials in Secret Manager
#   6. Grants required IAM roles to the Cloud Run service account
#   7. Deploys the Cloud Run service from source (~5-7 min)
#   8. Creates the Eventarc trigger (GCS → Cloud Run)
#   9. Seeds BigQuery with historical data
#  10. Prints all workshop act commands
#
# Prerequisites:
#   1. A GCP project with billing enabled
#   2. gcloud CLI authenticated:
#        gcloud auth login
#        gcloud config set project YOUR_PROJECT_ID
#   4. Slack Webhook URL + Signing Secret ready
#      --> See WORKSHOP.md "Step 1: Set Up Slack" before running this script
#
# Easiest option — Google Cloud Shell (zero local install, already authenticated):
#   https://shell.cloud.google.com
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC}  $*"; }
step()    { echo -e "${CYAN}[STEP]${NC}  $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
success() { echo -e "${GREEN}[✓]${NC}    $*"; }
err()     { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Configuration ─────────────────────────────────────────────────────────────
PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
REGION="${REGION:-us-central1}"
SERVICE_NAME="self-curation-engine"
BUCKET_NAME="landing-zone-${PROJECT_ID}"
BQ_DATASET="retail_curated"
BQ_TABLE="transactions"
GEMINI_MODEL="gemini-2.5-flash-lite"
AUTO_THRESHOLD="0.90"

[[ -z "$PROJECT_ID" ]] && err "PROJECT_ID not set. Run: gcloud config set project YOUR_PROJECT_ID"

echo ""
echo "========================================================"
echo "  Self-Curation Engine — Workshop Deployment"
echo "  Project : $PROJECT_ID"
echo "  Region  : $REGION"
echo "========================================================"
echo ""
echo "  Cloud Build takes ~5-7 min. You will be prompted for"
echo "  Slack credentials first — have them ready."
echo "  (See WORKSHOP.md Step 1 if you haven't set up Slack yet)"
echo "========================================================"
echo ""

# ── Collect Slack credentials ─────────────────────────────────────────────────
read -rp "  Slack Webhook URL (Enter to skip): " SLACK_WEBHOOK
read -rp "  Slack Signing Secret (Enter to skip): " SLACK_SECRET
echo ""
[[ -n "$SLACK_WEBHOOK" ]] \
    && info "Slack Webhook URL provided — HITL will fire Slack messages." \
    || warn "No Slack configured — HITL will use the /pending dashboard only."
echo ""

# ── Step 1: Enable APIs ────────────────────────────────────────────────────────
step "1/9  Enabling GCP APIs (takes ~60 s on first run)..."
gcloud services enable \
    aiplatform.googleapis.com \
    run.googleapis.com \
    eventarc.googleapis.com \
    firestore.googleapis.com \
    bigquery.googleapis.com \
    storage.googleapis.com \
    secretmanager.googleapis.com \
    pubsub.googleapis.com \
    cloudbuild.googleapis.com \
    --project="$PROJECT_ID" \
    --quiet
success "APIs enabled"

# ── Step 2: GCS bucket ────────────────────────────────────────────────────────
step "2/9  Creating GCS landing bucket: gs://$BUCKET_NAME"
if gsutil ls "gs://$BUCKET_NAME" &>/dev/null; then
    warn "Bucket already exists — skipping"
else
    gcloud storage buckets create "gs://$BUCKET_NAME" \
        --location="$REGION" \
        --project="$PROJECT_ID"
    success "Bucket created: gs://$BUCKET_NAME"
fi

# ── Step 3: BigQuery ──────────────────────────────────────────────────────────
step "3/9  Creating BigQuery dataset + table..."
bq --location="$REGION" mk \
    --dataset \
    --description "Self-Curation Engine — Retail Curation Layer" \
    "${PROJECT_ID}:${BQ_DATASET}" 2>/dev/null || warn "Dataset already exists"

bq mk \
    --table \
    --description "Curated retail transactions — AI-mapped schema" \
    "${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}" \
    schema.json 2>/dev/null || warn "Table already exists"
success "BigQuery ready: ${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}"

# ── Step 4: Firestore ─────────────────────────────────────────────────────────
step "4/9  Initialising Firestore (Native mode)..."
gcloud firestore databases create \
    --location="$REGION" \
    --type=firestore-native \
    --project="$PROJECT_ID" 2>/dev/null || warn "Firestore already initialised"
success "Firestore ready"

# ── Step 5: Slack secrets ─────────────────────────────────────────────────────
store_secret() {
    local name="$1" value="$2"
    if [[ -n "$value" ]]; then
        echo -n "$value" | gcloud secrets create "$name" \
            --data-file=- --project="$PROJECT_ID" 2>/dev/null || \
        echo -n "$value" | gcloud secrets versions add "$name" \
            --data-file=- --project="$PROJECT_ID"
        success "Secret stored: $name"
    else
        warn "No value for $name — skipping"
    fi
}

step "5/9  Storing Slack credentials in Secret Manager..."
store_secret "slack-webhook-url"    "$SLACK_WEBHOOK"
store_secret "slack-signing-secret" "$SLACK_SECRET"

# ── Step 6: IAM ───────────────────────────────────────────────────────────────
step "6/9  Granting IAM roles..."
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
GCS_SA="service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com"

ROLES=(
    "roles/aiplatform.user"
    "roles/bigquery.dataEditor"
    "roles/bigquery.jobUser"
    "roles/datastore.user"
    "roles/storage.objectViewer"
    "roles/secretmanager.secretAccessor"
    "roles/run.invoker"
)
for role in "${ROLES[@]}"; do
    gcloud projects add-iam-policy-binding "$PROJECT_ID" \
        --member="serviceAccount:$COMPUTE_SA" \
        --role="$role" \
        --condition=None \
        --quiet 2>/dev/null || true
done

# GCS service agent needs Pub/Sub publisher permission for Eventarc
gcloud beta services identity create \
    --service=storage.googleapis.com \
    --project="$PROJECT_ID" 2>/dev/null || true
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$GCS_SA" \
    --role="roles/pubsub.publisher" \
    --condition=None \
    --quiet 2>/dev/null || warn "pubsub.publisher grant failed — retry manually if Eventarc doesn't fire"
success "IAM roles granted"

# ── Step 7: Deploy Cloud Run service ──────────────────────────────────────────
step "7/9  Deploying Cloud Run service (Cloud Build compiling — ~5-7 min)..."

ENV_VARS="PROJECT_ID=${PROJECT_ID}"
ENV_VARS+=",LOCATION=${REGION}"
ENV_VARS+=",GEMINI_MODEL=${GEMINI_MODEL}"
ENV_VARS+=",BQ_DATASET=${BQ_DATASET}"
ENV_VARS+=",BQ_TABLE=${BQ_TABLE}"
ENV_VARS+=",AUTO_INGEST_THRESHOLD=${AUTO_THRESHOLD}"

SECRETS_FLAG=""
[[ -n "$SLACK_WEBHOOK" ]] && \
    SECRETS_FLAG="--set-secrets=SLACK_WEBHOOK_URL=slack-webhook-url:latest,SLACK_SIGNING_SECRET=slack-signing-secret:latest"

gcloud run deploy "$SERVICE_NAME" \
    --source . \
    --region "$REGION" \
    --platform managed \
    --service-account "$COMPUTE_SA" \
    --set-env-vars "$ENV_VARS" \
    ${SECRETS_FLAG:+$SECRETS_FLAG} \
    --allow-unauthenticated \
    --min-instances 0 \
    --max-instances 10 \
    --memory 512Mi \
    --timeout 120 \
    --project "$PROJECT_ID"

SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" \
    --region "$REGION" \
    --format 'value(status.url)' \
    --project "$PROJECT_ID")
success "Cloud Run deployed: $SERVICE_URL"

# Patch SERVICE_URL into env so logs print the exact curl approve command for Act 2
gcloud run services update "$SERVICE_NAME" \
    --update-env-vars "SERVICE_URL=${SERVICE_URL}" \
    --region "$REGION" \
    --project "$PROJECT_ID" \
    --quiet

# ── Step 8: Eventarc trigger ──────────────────────────────────────────────────
step "8/9  Creating Eventarc trigger (GCS → Cloud Run)..."
TRIGGER_NAME="${SERVICE_NAME}-gcs-trigger"
gcloud eventarc triggers create "$TRIGGER_NAME" \
    --location="$REGION" \
    --destination-run-service="$SERVICE_NAME" \
    --destination-run-region="$REGION" \
    --event-filters="type=google.cloud.storage.object.v1.finalized" \
    --event-filters="bucket=$BUCKET_NAME" \
    --service-account="$COMPUTE_SA" \
    --project="$PROJECT_ID" 2>/dev/null || warn "Trigger may already exist — skipping"
success "Eventarc trigger created"

# ── Step 9: Seed BigQuery ─────────────────────────────────────────────────────
step "9/9  Seeding BigQuery with historical data..."
bq load \
    --source_format=NEWLINE_DELIMITED_JSON \
    --project_id="$PROJECT_ID" \
    --location="$REGION" \
    "${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE}" \
    ./data/seed_data.jsonl 2>/dev/null || warn "Seed load skipped (table may already have data)"
success "Seed data loaded"

# ══════════════════════════════════════════════════════════════════════════════
# DEPLOYMENT COMPLETE — WORKSHOP ACTS
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "========================================================"
echo -e "  ${GREEN}DEPLOYMENT COMPLETE${NC}"
echo "========================================================"
echo ""
echo "  Project   : $PROJECT_ID"
echo "  Bucket    : gs://$BUCKET_NAME"
echo "  Service   : $SERVICE_URL"
echo "  Dashboard : $SERVICE_URL/pending"
echo ""

if [[ -n "$SLACK_WEBHOOK" ]]; then
    echo -e "  ${GREEN}✅ Slack: ENABLED${NC}"
    echo ""
    echo "  ── ACTION REQUIRED — finish Slack setup: ─────────────"
    echo "  Open your Slack App settings and set the Interactivity"
    echo "  Request URL so the Approve button works:"
    echo ""
    echo "    https://api.slack.com/apps"
    echo "    → Your app → Interactivity & Shortcuts → ON"
    echo "    → Request URL: $SERVICE_URL/slack/interactive"
    echo "    → Save Changes"
    echo ""
else
    echo -e "  ${YELLOW}⚠️  Slack: not configured${NC}"
    echo "  HITL cards will appear on the /pending dashboard only."
    echo "  Re-run this script to add Slack credentials later."
    echo ""
fi

echo "========================================================"
echo "  BEFORE RUNNING ACTS: open the log stream"
echo "  (in a separate terminal tab)"
echo ""
echo "    gcloud beta logging tail \\"
echo "      'resource.type=cloud_run_revision AND resource.labels.service_name=$SERVICE_NAME' \\"
echo "      --project=$PROJECT_ID"
echo ""
echo "  Also open the dashboard in your browser:"
echo "    $SERVICE_URL/pending"
echo ""
echo "  NOTE: Wait ~60 s before running Act 1 — Eventarc needs"
echo "  a moment to propagate after first creation."
echo "========================================================"
echo ""
echo "  ── ACT 1: Auto-ingest (high-confidence schema) ──────"
echo "  Headers are cryptic but guessable. Gemini maps them."
echo "  Confidence >= 90% -> auto-ingest to BigQuery."
echo ""
echo "    gcloud storage cp data/legacy_pos_export_v1.csv \\"
echo "      gs://${BUCKET_NAME}/legacy_pos_export_v1.csv"
echo ""
echo "  Then query results:"
echo "    bq query --use_legacy_sql=false --project_id=${PROJECT_ID} --location=${REGION} \\"
echo "      'SELECT transaction_id, store_name, total_amount, _metadata.from_cache"
echo "       FROM \`${PROJECT_ID}.${BQ_DATASET}.${BQ_TABLE}\`"
echo "       ORDER BY _metadata.processed_at DESC LIMIT 10'"
echo ""
echo "  ── ACT 2: HITL (ambiguous schema) ───────────────────"
echo "  Headers: Column_A, Column_B, Column_C, Column_D"
echo "  Confidence < 90% -> Slack message + dashboard card."
echo ""
echo "    gcloud storage cp data/ambiguous_schema.csv \\"
echo "      gs://${BUCKET_NAME}/ambiguous_schema.csv"
echo ""
echo "  Then approve via Slack, or open the dashboard:"
echo "    $SERVICE_URL/pending"
echo ""
echo "  Backup — approve via curl (Pending ID is in the logs):"
echo "    curl '${SERVICE_URL}/approve?id=PASTE_PENDING_ID_HERE'"
echo ""
echo "  ── ACT 3: Cache hit (same schema, zero LLM cost) ────"
echo "  Same headers as Act 1. Firestore recognises them."
echo "  No Gemini call — from_cache=true in BigQuery."
echo ""
echo "    gcloud storage cp data/legacy_pos_export_v1.csv \\"
echo "      gs://${BUCKET_NAME}/act3_replay.csv"
echo ""
echo "  ── CLEANUP (when done) ──────────────────────────────"
echo "    chmod +x scripts/cleanup.sh && ./scripts/cleanup.sh"
echo ""
echo "========================================================"
