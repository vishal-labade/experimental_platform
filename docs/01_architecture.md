# Experimentation & Causal Inference Platform — Architecture

## Purpose

This document explains **why the platform is designed the way it is**.
It focuses on tradeoffs, constraints, and decisions — not implementation trivia.

The system is intentionally **local-first, reproducible, and audit-friendly**.

---

## High-Level Architecture

```
Synthetic / Real Events
        ↓
Raw Parquet (Object Storage)
        ↓
Canonical Iceberg Tables
        ↓
Metrics Aggregation Layer
        ↓
Inference & Causal Methods
        ↓
Decision Memo
```

---

## Why Iceberg

Iceberg is the backbone of the platform.

**Chosen because it provides:**
- Atomic writes (safe reruns)
- Schema evolution
- Time travel (auditability)
- Partition-level overwrite

**Why not plain Parquet or a DB?**
- Experiments are append-heavy but correction-prone
- We need reproducibility, not low-latency serving

---

## Why Idempotency Is Non-Negotiable

Experiments are rerun frequently:
- logic changes
- bugs fixed
- metrics redefined

**Design invariant**
> Re-running an experiment must never corrupt unrelated data.

Achieved via:
- experiment_id–scoped overwrites
- deterministic keys
- immutable raw inputs

---

## Why a Metrics Layer (Not Inline SQL)

Metrics are **organizational contracts**, not queries.

Centralizing metrics ensures:
- consistent definitions
- guardrails are enforced
- historical results remain interpretable

This mirrors how large product orgs avoid “metric drift.”

---

## Why Multiple Inference Methods

No single method works everywhere.

| Scenario | Method |
|-------|-------|
| Clean randomization | A/B |
| Noisy metrics | CUPED |
| Partial rollout / policy change | DiD |

The platform encodes **method choice as data reality**, not preference.

---

## Why Local-First

Local-first is intentional.

Benefits:
- Fast iteration
- No infra friction
- Easy reproducibility
- Hiring-manager inspectability

The architecture is cloud-portable, but **debugging is local**.

---

## Failure Semantics

Certain outcomes are *valid*, not errors:
- INSUFFICIENT_DATA
- Failed pre-trends
- Guardrail degradation

The platform records these explicitly instead of hiding them.

---

## Design North Star

> If leadership asks “Can we trust this result?”  
> the system should make the answer obvious.