# Self-Curation Engine — Workshop Guide
### GDG Brisbane · Build with AI · Hands-On Session

---

## What You'll Build

A fully serverless pipeline that uses Gemini Flash to automatically map messy legacy CSV headers to a clean BigQuery schema — with a Human-in-the-Loop fallback delivered via **Slack** when the AI isn't confident enough.

```
GCS upload → Eventarc → Cloud Run → Gemini Flash
                                        ↓
                            confidence ≥ 90% → BigQuery (auto)
                            confidence < 90% → Slack message + approve
                                                     ↓
                                              Firestore memory
                                              (next same file = zero LLM cost)
```

---

## Pre-Flight Checklist

Complete all three items **before** running the deploy script.

- [ ] GCP project with billing enabled
- [ ] gcloud CLI authenticated and project set
- [ ] Slack workspace + app created (Webhook URL + Signing Secret in hand)

---

## Step 1 — Set Up Slack

> Do this first. The deploy script will prompt you for your Slack credentials — have them ready before you run it.

### 1a. Create a Slack workspace (skip if you already have one)

1. Go to [https://slack.com/get-started](https://slack.com/get-started)
2. Click **Create a new workspace**
3. Sign in with your email — follow the prompts
4. Name your workspace (e.g. `SCE Workshop`) and create a channel (e.g. `#sce-alerts`)

> A free Slack workspace is sufficient. You only need one workspace per team — if someone in your group already has one, ask them to add you instead of creating a new one.

### 1b. Create a Slack App

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps)
2. Click **Create New App** → **From scratch**
3. Set **App Name** to `Self-Curation Engine`
4. Select your workspace from the dropdown
5. Click **Create App**

### 1c. Enable Incoming Webhooks and get your Webhook URL

1. In the left sidebar of your app settings, click **Incoming Webhooks**
2. Toggle **Activate Incoming Webhooks** to **On**
3. Scroll down and click **Add New Webhook to Workspace**
4. Choose the channel where HITL alerts should appear (e.g. `#sce-alerts`)
5. Click **Allow**
6. **Copy the Webhook URL** — it looks like:
   ```
   https://hooks.slack.com/services/T.../B.../xxxxxxx
   ```
   Save this somewhere — you'll paste it into the deploy script shortly.

### 1d. Copy your Signing Secret

1. In the left sidebar, click **Basic Information**
2. Scroll to **App Credentials**
3. Click **Show** next to **Signing Secret**
4. **Copy the value** and keep it alongside your Webhook URL

> **Why the Signing Secret?** When you click Approve in a Slack message, Slack sends a POST to your Cloud Run service. The Signing Secret lets the service verify that request actually came from Slack (HMAC-SHA256) and not an outside caller.

### 1e. Note: one more Slack step happens AFTER deploy

After the deploy script finishes, it will print your Cloud Run `SERVICE_URL`. You'll need to paste that into your Slack App settings to wire up the Approve button. The deploy script output will walk you through it — you can't do it now because the URL doesn't exist yet.

---

## Step 2 — Get the Code

**Easiest: use Google Cloud Shell** — it has `gcloud`, Python, and `bq` pre-installed, and you're already authenticated to GCP:

```
https://shell.cloud.google.com
```

Then clone the repo:

```bash
git clone https://github.com/YOUR_USERNAME/self-curation-engine.git
cd "self-curation-engine GDG Brisbane"
```

> Replace `YOUR_USERNAME` with the GitHub username provided at the workshop.

If running locally instead:

```bash
# Install gcloud: https://cloud.google.com/sdk/docs/install
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

---

## Step 3 — Deploy Everything

```bash
chmod +x workshop_deploy.sh
./workshop_deploy.sh
```

The script will prompt you for the two Slack values from Step 1:

```
Slack Webhook URL (Enter to skip): https://hooks.slack.com/services/...
Slack Signing Secret (Enter to skip): abc123...
```

Then it runs unattended for ~5-7 minutes while Cloud Build compiles the container. It creates:

| Resource | Name |
|---|---|
| GCS Bucket | `landing-zone-YOUR_PROJECT_ID` |
| BigQuery dataset | `retail_curated` → table `transactions` |
| Firestore | Native mode, default database |
| Secret Manager | `slack-webhook-url`, `slack-signing-secret` |
| Cloud Run | `self-curation-engine` |
| Eventarc trigger | GCS Object.Finalized → Cloud Run |

At the end it prints your `SERVICE_URL` and all three workshop act commands pre-filled with your values.

### Step 3a — Wire up the Slack Approve button

The deploy script will print this reminder, but do it now while you wait. Back in your Slack App settings:

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps) → your app
2. Left sidebar → **Interactivity & Shortcuts**
3. Toggle **Interactivity** to **On**
4. Paste your `SERVICE_URL` into the **Request URL** field:
   ```
   https://YOUR-SERVICE-URL.run.app/slack/interactive
   ```
5. Click **Save Changes**

This wires the **Approve** button in Slack messages back to your Cloud Run service.

---

## Step 4 — Open Log Stream + Dashboard

Before running any acts, open a **second terminal** and tail live logs:

```bash
gcloud beta logging tail \
  'resource.type=cloud_run_revision AND resource.labels.service_name=self-curation-engine' \
  --project=YOUR_PROJECT_ID
```

Also open the HITL dashboard in your browser — it auto-refreshes every 5 seconds:

```
https://YOUR-SERVICE-URL.run.app/pending
```

> **Wait ~60 seconds** after deploy before running Act 1. Eventarc needs time to propagate on first creation.

---

## The Workshop Acts

The deploy script already printed your exact act commands. Here's what each act demonstrates.

---

### Act 1 — Auto-ingest (high confidence)

**File:** `data/legacy_pos_export_v1.csv`
**Headers:** `TXN_REF, STORE_LOC_ID, VAL_EX_TAX, CURRENCY, D_TIME_ISO`

Cryptic but guessable. Gemini maps them semantically and returns confidence ≥ 90% — no human needed.

```bash
gcloud storage cp data/legacy_pos_export_v1.csv \
  gs://landing-zone-YOUR_PROJECT_ID/legacy_pos_export_v1.csv
```

**Watch the logs:**
```
[CACHE MISS] Calling gemini-2.5-flash-lite for schema reasoning…
global_confidence=0.95  status=AUTO_APPROVE
Staging load complete: 5 rows → ...staging_...
Curated insert complete → retail_curated.transactions (Gemini-mapped)
Mapping saved to memory (AUTO_APPROVE, fingerprint=...)
```

**Query BigQuery:**
```bash
bq query --use_legacy_sql=false \
  --project_id=YOUR_PROJECT_ID --location=us-central1 \
  "SELECT transaction_id, store_name, total_amount,
          _metadata.from_cache, _metadata.mapping_confidence
   FROM \`YOUR_PROJECT_ID.retail_curated.transactions\`
   ORDER BY _metadata.processed_at DESC LIMIT 10"
```

`from_cache=false`, `mapping_confidence ≈ 0.95`.

---

### Act 2 — HITL (ambiguous schema)

**File:** `data/ambiguous_schema.csv`
**Headers:** `Column_A, Column_B, Column_C, Column_D`

Gemini has almost no signal from the headers — values are the only clue. Confidence < 90% triggers HITL.

```bash
gcloud storage cp data/ambiguous_schema.csv \
  gs://landing-zone-YOUR_PROJECT_ID/ambiguous_schema.csv
```

**Two things happen simultaneously:**

A Slack message fires in your channel:
```
🤖 Ambiguous Mapping Detected
File: ambiguous_schema.csv    Confidence: 82.0% ⚠️

Column_A → transaction_id    (0.95)
Column_B → event_timestamp   (0.90)
Column_C → total_amount      (0.95)
Column_D → currency_code     (0.75)

[ ✅ Approve & Save ]  [ ✏ Edit Manually ]
```

And a card appears on the `/pending` dashboard in your browser.

**Approve via Slack** — click **Approve & Save** in the message.

**Or approve via the dashboard** — open `$SERVICE_URL/pending` and click the Approve button on your card.

**Backup — approve via curl** (copy the Pending ID from logs):
```bash
curl "https://YOUR-SERVICE-URL.run.app/approve?id=PASTE_PENDING_ID_HERE"
```

The approved mapping is saved to Firestore — every future file with the same headers will skip Gemini.

---

### Act 3 — Cache hit (zero LLM cost)

Act 1 wrote the `legacy_pos_export_v1.csv` header fingerprint to Firestore. Upload it again under a different name:

```bash
gcloud storage cp data/legacy_pos_export_v1.csv \
  gs://landing-zone-YOUR_PROJECT_ID/act3_replay.csv
```

**Watch the logs:**
```
[CACHE HIT] Fingerprint abc123de… — skipping Gemini call.
Curated insert complete → retail_curated.transactions (from cache)
```

**Query again** — notice `from_cache=true` for the new rows. Same vendor schema, zero LLM cost.

---

## Step 5 — Clean Up

Delete all GCP resources when you're done to avoid ongoing charges:

```bash
chmod +x scripts/cleanup.sh && ./scripts/cleanup.sh
```

Deletes: Cloud Run service, Eventarc trigger, GCS bucket, BigQuery dataset, Firestore collections, Slack secrets from Secret Manager.

---

## Explore and Extend (optional)

**Force Act 1 to HITL** — raise the confidence threshold:
```bash
gcloud run services update self-curation-engine \
  --update-env-vars AUTO_INGEST_THRESHOLD=0.99 \
  --region us-central1 --project YOUR_PROJECT_ID

chmod +x scripts/reset_demo.sh && ./scripts/reset_demo.sh
gcloud storage cp data/legacy_pos_export_v1.csv \
  gs://landing-zone-YOUR_PROJECT_ID/
```

**Add a field to the target schema** — edit `config.py → TARGET_SCHEMA`, add a `product_sku` field, redeploy, and watch Gemini try to find it in `ambiguous_schema.csv`.

**Inspect the Firestore cache** — GCP Console → Firestore → `mapping_memory` collection. Each document is one vendor fingerprint with the full approved mapping JSON.

---

## Cost Estimate

| Resource | Workshop usage | Cost |
|---|---|---|
| Cloud Run (min-instances=0) | Build + 3 invocations | ~$0.02 |
| Vertex AI Gemini Flash Lite | 2 calls (Acts 1 & 2) | ~$0.001 |
| GCS, BigQuery, Firestore | Minimal I/O | < $0.01 |
| **Total** | **Full workshop** | **< $0.05** |

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Eventarc doesn't fire after upload | Wait 60-90 s — Eventarc propagation has a warm-up delay after first creation |
| No Slack message in Act 2 | Run `gcloud run services describe self-curation-engine --region us-central1 --format 'value(spec.template.spec.containers[0].env)'` — check `SLACK_WEBHOOK_URL` is set |
| Slack Approve button does nothing / errors | Go to api.slack.com/apps → your app → Interactivity & Shortcuts → confirm Request URL is `$SERVICE_URL/slack/interactive` |
| `[CACHE HIT]` in Act 1 (should be a miss) | Run `./scripts/reset_demo.sh` to clear Firestore, then re-upload |
| `404 Publisher Model … gemini-2.5-flash` | Check `GEMINI_MODEL` env var on the service — should be `gemini-2.5-flash-lite` |
| `403` in Cloud Run logs | IAM propagation lag — wait 60 s and re-upload |
| BigQuery rows not appearing | Use `ORDER BY _metadata.processed_at DESC` — avoid the Console Preview tab |
| Cloud Build fails | Check: `gcloud builds list --project=YOUR_PROJECT_ID --limit=5` |

---

## Architecture Decisions Worth Discussing

**Why Firestore for the cache and not BigQuery or Redis?**
Firestore is the cheapest always-on key-value store in GCP — millisecond point-reads, no minimum instance cost. Redis adds ~$50/month. BigQuery isn't designed for point-lookups.

**Why a Load Job to BigQuery, not streaming inserts?**
Load Jobs are atomic — all rows appear at once after the job completes. Streaming inserts cost more per row and are eventually consistent. For batch file ingestion, Load Jobs are the right tool.

**Why return HTTP 200 on error from the Cloud Run handler?**
Eventarc retries any non-2xx response. If the handler crashes on a malformed file you don't want infinite retries flooding Gemini with the same broken payload. Log the error, return 200, move on.

**Why both Slack AND the `/pending` dashboard?**
They serve different audiences. Slack delivers the proposal to your inbox — approve without opening a browser. The dashboard is a live projector view and works for anyone without a Slack account. Both fire independently; either can approve.

**Why `response_schema` on the Gemini call?**
Without it, Gemini returns free-text and you're parsing fragile strings. `response_schema` constrains the output to an exact JSON structure — `mappings[]`, `global_status`, `global_confidence` — so the pipeline never breaks on an unexpected response format.

**What would it cost at scale?**
Say 1,000 vendors each upload a daily file. After the first month all 1,000 fingerprints are cached. Every subsequent day: 1,000 cache hits, 1,000 BigQuery load jobs, zero Gemini calls. Daily cost: ~$0.10 in BigQuery compute.

---

## Resources

- **Gemini on Vertex AI:** [cloud.google.com/vertex-ai/generative-ai/docs](https://cloud.google.com/vertex-ai/generative-ai/docs)
- **Eventarc:** [cloud.google.com/eventarc/docs](https://cloud.google.com/eventarc/docs)
- **Cloud Run pricing:** [cloud.google.com/run/pricing](https://cloud.google.com/run/pricing)
- **Slack Incoming Webhooks:** [api.slack.com/messaging/webhooks](https://api.slack.com/messaging/webhooks)
- **Slack Interactivity:** [api.slack.com/interactivity](https://api.slack.com/interactivity)
- **Google Cloud Shell:** [shell.cloud.google.com](https://shell.cloud.google.com)
