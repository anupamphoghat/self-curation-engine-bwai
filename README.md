# 🤖 Self-Curation Engine
### Agentic Schema Mapping with Gemini & Cloud Run

> *"Stop writing mappings. Start building reasoning layers."*
>
> As presented at **Google Cloud Next '26** — Developer Theater

---

## What This Is

The **Self-Curation Engine** is a serverless reference architecture that eliminates the most manual part of data engineering: **schema mapping**.

When a legacy CSV or JSON file lands in Cloud Storage, the engine:

1. **Peeks** at the first 10 KB
2. **Checks Firestore** for a known mapping fingerprint (cache-first, zero LLM cost)
3. If unknown, calls **Gemini Flash** to semantically map raw headers to your target schema
4. **Auto-ingests** if confidence ≥ 90% — or routes to **Slack** for human approval
5. **Remembers** every human decision in Firestore so it never asks the same question twice

The result: a pipeline that adapts to schema drift automatically, costs fractions of a cent per file, and gets faster the more you use it.

---

## Architecture

```
GCS Bucket  →  Eventarc  →  Cloud Run Service
                                │
                    ┌───────────┴───────────────┐
                    │   1. Peek (first 10 KB)   │
                    │   2. Firestore cache check │
                    │   3. Gemini Flash call     │
                    │   4. Confidence gate       │
                    │   5. SQL Hydration         │
                    └───────┬───────────┬────────┘
                            │           │
                   conf ≥ 90%         conf < 90%
                            │           │
                        BigQuery     Slack HITL
                     (auto-ingest)  + Firestore
                                     (pending)
                                         │
                                   Human approves
                                         │
                               Firestore Memory + BQ
```

**Key design decisions:**
- **CloudEvents SDK** for correct Eventarc payload parsing (not `request.get_json()`)
- **BigQuery Load Jobs** for atomic, visible ingestion (not streaming inserts)
- **Sorted SHA-256 header fingerprint** for order-independent cache keys
- **Slack HMAC-SHA256** signing secret verification (not deprecated token)
- **`response_schema` enforcement** on Gemini output — structured JSON guaranteed
- **`SAFE_CAST` + `COALESCE`** defensive SQL — pipeline never crashes on bad data

---

## Repository Structure

```
self-curation-engine/
├── main.py                  # Cloud Run service (Eventarc handler + Slack callback)
├── config.py                # All configuration — edit TARGET_SCHEMA here
├── requirements.txt         # Python dependencies
├── env.yaml                 # Environment variable template
├── schema.json              # BigQuery table schema (DDL)
├── deploy.sh                # One-command GCP deployment
├── data/
│   ├── legacy_pos_export_v1.csv   # Demo: Act 1 (auto-ingest)
│   ├── ambiguous_schema.csv       # Demo: Act 2 (HITL trigger)
│   └── seed_data.jsonl            # BigQuery historical seed data
└── scripts/
    ├── seed_firestore.py          # Pre-seeds Mapping Memory for Act 3
    └── reset_demo.sh              # Clears state before each demo run
```

---

## Quick Start

### Prerequisites
- Google Cloud project with billing enabled
- `gcloud` CLI authenticated: `gcloud auth login`
- Python 3.11+
- Slack app with **Incoming Webhooks** and **Interactivity** enabled

### 1. Clone

```bash
git clone https://github.com/YOUR_USERNAME/self-curation-engine.git
cd self-curation-engine
```

### 2. Deploy Everything

```bash
chmod +x deploy.sh
./deploy.sh
```

The script will prompt for your Slack Webhook URL and Signing Secret, then provision all GCP resources automatically.

**What gets created:**
| Resource | Name |
|---|---|
| GCS Bucket | `landing-zone-{PROJECT_ID}` |
| BigQuery Dataset | `retail_curated` |
| BigQuery Table | `retail_curated.transactions` |
| Firestore DB | Native Mode |
| Cloud Run Service | `self-curation-engine` |
| Eventarc Trigger | `self-curation-engine-gcs-trigger` |

### 3. Configure Your Target Schema

Edit `config.py` to define the "gold standard" schema Gemini maps toward:

```python
TARGET_SCHEMA = {
    "transaction_id": {
        "description": "Unique transaction identifier",
        "bq_type": "STRING",
        "example_values": ["AU-99283", "TXN-7721"]
    },
    "store_name": {
        "description": "Store name or location code",
        "bq_type": "STRING",
        "example_values": ["SYD_CBD_01", "Melbourne East"]
    },
    # ... add your own fields
}
```

### 4. Configure Slack Interactivity

In your Slack App Dashboard → **Interactivity & Shortcuts → Request URL**, set:

```
https://YOUR_CLOUD_RUN_URL/slack/interactive
```

This is where Slack sends the button-click callback when a data steward approves a mapping.

### 5. Run the Demo

```bash
# Reset state to a clean baseline
./scripts/reset_demo.sh

# Act 1: Auto-ingest (high confidence)
gcloud storage cp data/legacy_pos_export_v1.csv gs://landing-zone-${PROJECT_ID}/

# Act 2: HITL (low confidence — watch Slack)
gcloud storage cp data/ambiguous_schema.csv gs://landing-zone-${PROJECT_ID}/

# Act 3: Cache hit (re-upload Act 1 file — should be instant, no Gemini call)
gcloud storage cp data/legacy_pos_export_v1.csv gs://landing-zone-${PROJECT_ID}/act3_test.csv
```

Watch logs in real time:
```bash
gcloud beta logging tail \
  'resource.type=cloud_run_revision AND resource.labels.service_name=self-curation-engine' \
  --project=$PROJECT_ID
```

---

## How the Mapping Memory Works

The engine uses a **sorted, normalised SHA-256 fingerprint** of the file's column headers as a Firestore document key.

```python
def compute_header_fingerprint(headers: list[str]) -> str:
    normalised = ",".join(sorted(h.strip().upper() for h in headers))
    return hashlib.sha256(normalised.encode()).hexdigest()
```

**Why sorted?** Some CSV exporters shuffle column order between runs. Sorting makes the cache order-independent — the same vendor's file always hits the same cache entry regardless of column sequence.

**Cache flow:**
```
New file arrives
    → compute fingerprint
    → Firestore lookup (< 100 ms)
    → HIT: use cached mapping → BigQuery Load Job
    → MISS: call Gemini Flash → confidence gate → save/route
```

---

## Security

| Threat | Mitigation |
|---|---|
| **Prompt injection** via CSV values | System prompt role-anchoring + JSON schema enforcement |
| **SQL injection** via field values | `SAFE_CAST()` returns NULL — never executes raw strings |
| **Replay attacks** on Slack webhook | HMAC-SHA256 timestamp validation (5-min window) |
| **Unauthorised Slack callbacks** | Signing secret verification on every `/slack/interactive` request |
| **Data exfiltration via Gemini** | Vertex AI tenant isolation — your data never trains foundation models |

---

## Cost Model

| Scenario | Gemini Cost | Time |
|---|---|---|
| Known vendor (cache hit) | **$0.00000** | ~3 s |
| New vendor, auto-approved | ~$0.0002 | ~5 s |
| New vendor, HITL required | ~$0.00015 | ~45 s (incl. human) |
| Traditional ETL schema break | $0 (but 4–6 eng-hours) | Hours |

The cache compounds over time. Once a vendor's schema is approved, every future file from that vendor costs nothing and completes in seconds.

---

## Extending the Engine

**Add a new source format (JSON)**

The `extract_headers()` function in `main.py` already handles JSON. To add XML or Parquet, add a new branch:

```python
def extract_headers(sample_data: str, file_name: str) -> list[str]:
    if file_name.lower().endswith(".parquet"):
        # Use pyarrow to read schema
        ...
```

**Change the confidence threshold**

Set `AUTO_INGEST_THRESHOLD` in `env.yaml` or export it before deploying:

```yaml
AUTO_INGEST_THRESHOLD: "0.85"   # More automation, higher risk
AUTO_INGEST_THRESHOLD: "0.95"   # Stricter human review
```

**Add Dataform integration**

After BigQuery ingestion, trigger a Dataform workflow by adding a `trigger_dataform()` call in `auto_ingest()` in `main.py`.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| Eventarc doesn't fire | Run `gcloud beta services identity create --service=storage.googleapis.com` and re-grant `pubsub.publisher` to the GCS service agent |
| `403` on Cloud Run | Ensure `roles/run.invoker` is granted to the Eventarc service account |
| Gemini returns text instead of JSON | Verify `response_schema` is set in `GenerationConfig` and `response_mime_type="application/json"` |
| Slack "Approve" button does nothing | Check Request URL is set in Slack App → Interactivity & Shortcuts |
| BigQuery rows not visible immediately | Use `ORDER BY event_timestamp DESC LIMIT 10` — avoid the Preview tab which may show cached results |

---

## Talk Resources

- **Session:** The Self-Curation Engine: Real-Time Schema Mapping with Gemini & Cloud Run
- **Event:** GDG Brisbane — Build with AI Workshop
- **Workshop Guide:** [WORKSHOP.md](WORKSHOP.md)

---

## License

# License — see [LICENSE](LICENSE) for details.

> **Disclaimer:** This is a reference architecture for demonstration purposes. Load-test before production use and review your organisation's data governance policies.
