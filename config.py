"""
config.py — Self-Curation Engine Configuration
------------------------------------------------
Single source of truth for all environment-driven settings.
Update TARGET_SCHEMA to define your own curation standard.
"""
import os

# ── Google Cloud Project ───────────────────────────────────────
# Set via the PROJECT_ID environment variable — workshop_deploy.sh handles this.
PROJECT_ID = os.environ.get("PROJECT_ID", "")
LOCATION   = os.environ.get("LOCATION",   "us-central1")

# ── Gemini Model ───────────────────────────────────────────────
# Verify current model strings at:
# https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/gemini
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash-lite")

# ── BigQuery Destination ───────────────────────────────────────
BQ_DATASET = os.environ.get("BQ_DATASET", "retail_curated")
BQ_TABLE   = os.environ.get("BQ_TABLE",   "transactions")

# ── Confidence Threshold ───────────────────────────────────────
# Files with global_confidence >= this value are auto-ingested.
# Files below are routed to Human-in-the-Loop.
AUTO_INGEST_THRESHOLD = float(os.environ.get("AUTO_INGEST_THRESHOLD", "0.90"))

# ── Firestore Collections ──────────────────────────────────────
FS_MEMORY_COLLECTION  = "mapping_memory"    # approved / auto-confirmed mappings
FS_PENDING_COLLECTION = "pending_mappings"  # awaiting human review

# ── Target Schema ──────────────────────────────────────────────
# This is the "Gold Standard" your curation layer enforces.
# The Gemini System Prompt includes this definition so the model
# knows exactly what fields it is mapping toward.
#
# Format: { "field_name": { "description": "...", "bq_type": "BIGQUERY_TYPE" } }
TARGET_SCHEMA = {
    "transaction_id": {
        "description": "Unique identifier for the transaction or item record.",
        "bq_type": "STRING",
        "example_values": ["AU-99283", "TXN-7721", "100234"]
    },
    "store_name": {
        "description": (
            "Human-readable store name or location code. "
            "If the source has separate region + code columns, "
            "suggest CONCAT(region, '_', code)."
        ),
        "bq_type": "STRING",
        "example_values": ["SYD_CBD_01", "Melbourne East", "BNE-NORTH-02"]
    },
    "total_amount": {
        "description": (
            "Final price, sale value, or quantity. "
            "May be labelled as MSRP, Val, Price, AMT, or similar."
        ),
        "bq_type": "NUMERIC",
        "example_values": [145.50, 22.00, 310.99]
    },
    "currency_code": {
        "description": "ISO 4217 currency code. Default to 'AUD' if not present.",
        "bq_type": "STRING",
        "example_values": ["AUD", "USD"]
    },
    "event_timestamp": {
        "description": (
            "When the record was created or the transaction occurred. "
            "Handle mixed formats: ISO-8601, DD/MM/YYYY, epoch seconds."
        ),
        "bq_type": "TIMESTAMP",
        "example_values": ["2026-03-28T08:00:00Z", "28/03/2026 08:05:00"]
    },
}
