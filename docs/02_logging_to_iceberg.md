# Logging to Iceberg (Canonical Storage)

## Purpose
This layer establishes **immutable, auditable storage** for experimentation data.

Iceberg is chosen to:
- support safe overwrites
- enable time travel
- allow late data correction
- mirror lakehouse experimentation stacks

---

## Ingest Flow

```
Raw Parquet
  → Validation
  → Deduplication
  → Canonical Write (Iceberg)
```

Pipelines:
- ingest_registry.py
- ingest_exposures.py
- ingest_outcomes.py

---

## Idempotency Strategy

Each ingest uses:
- experiment_id scoped overwrite
- partition-aware replace
- deterministic keys

Rerunning ingest:
- replaces only affected rows
- never duplicates data
- preserves unrelated experiments

---

## Data Quality Checks

The DQ report surfaces:
- missing keys
- duplicate rows
- timestamp leakage
- row count deltas

Failures are **recorded**, not silently dropped.

---

## Definition of Done

- Raw → Iceberg is one command
- Tables are queryable immediately
- Reruns are safe
- DQ metrics are visible