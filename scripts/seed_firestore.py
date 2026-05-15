"""
scripts/seed_firestore.py
--------------------------
Pre-populates Firestore Mapping Memory so the "Act 3 — Cache Hit"
demo works immediately without waiting for a human approval.

Run ONCE from Cloud Shell before the demo:
  python scripts/seed_firestore.py

Requires: google-cloud-firestore, Application Default Credentials
  gcloud auth application-default login
"""

import sys
import hashlib
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from google.cloud import firestore
import config

db = firestore.Client(project=config.PROJECT_ID)


def compute_fingerprint(headers: list[str]) -> str:
    normalised = ",".join(sorted(h.strip().upper() for h in headers))
    return hashlib.sha256(normalised.encode()).hexdigest()


# ── Seed mapping for legacy_pos_export_v1.csv ─────────────────────────────────
headers_v1 = ["TXN_REF", "STORE_LOC_ID", "VAL_EX_TAX", "CURRENCY", "D_TIME_ISO"]
fp_v1 = compute_fingerprint(headers_v1)

mapping_v1 = {
    # ── Wrapped under "mapping" key to match the structure written by
    # auto_ingest() Step 5 and handle_slack_callback() — main.py reads
    # cached["mapping"] when it finds a Firestore hit.
    "mapping": {
        "mappings": [
            {
                "raw_header": "TXN_REF",
                "target_field": "transaction_id",
                "transformation_logic": "SAFE_CAST(TXN_REF AS STRING)",
                "confidence": 0.98,
                "reasoning": "TXN prefix + UUID-style values (AU-XXXXX) → transaction identifier"
            },
            {
                "raw_header": "STORE_LOC_ID",
                "target_field": "store_name",
                "transformation_logic": "SAFE_CAST(STORE_LOC_ID AS STRING)",
                "confidence": 0.95,
                "reasoning": "LOC + Australian city codes (SYD_CBD_01, MELB_EAST_04) → store location"
            },
            {
                "raw_header": "VAL_EX_TAX",
                "target_field": "total_amount",
                "transformation_logic": "COALESCE(SAFE_CAST(VAL_EX_TAX AS NUMERIC), 0.0)",
                "confidence": 0.93,
                "reasoning": "VAL = value, EX_TAX = excluding tax → numeric sale amount"
            },
            {
                "raw_header": "CURRENCY",
                "target_field": "currency_code",
                "transformation_logic": "SAFE_CAST(CURRENCY AS STRING)",
                "confidence": 0.99,
                "reasoning": "Direct ISO 4217 currency code match"
            },
            {
                "raw_header": "D_TIME_ISO",
                "target_field": "event_timestamp",
                "transformation_logic": "SAFE_CAST(D_TIME_ISO AS TIMESTAMP)",
                "confidence": 0.88,
                "reasoning": "D_TIME = date/time, mixed ISO formats detected — SAFE_CAST handles gracefully"
            },
        ],
        "global_status": "AUTO_APPROVE",
        "global_confidence": 0.95,
    },
    "approved_by": "System_Pre_Seed",
    "source_file": "legacy_pos_export_v1.csv",
}

db.collection(config.FS_MEMORY_COLLECTION).document(fp_v1).set(mapping_v1)
print(f"✅ Seeded mapping for legacy_pos_export_v1.csv")
print(f"   Fingerprint: {fp_v1[:16]}…")
print(f"   Headers:     {headers_v1}")
print()

# ── Verify ────────────────────────────────────────────────────────────────────
doc = db.collection(config.FS_MEMORY_COLLECTION).document(fp_v1).get()
if doc.exists:
    data = doc.to_dict()
    print("✅ Firestore read-back: document confirmed.")
    print(f"   global_confidence: {data['mapping']['global_confidence']}")
    print(f"   approved_by:       {data['approved_by']}")
else:
    print("❌ ERROR: Document not found after write — check Firestore permissions.")
