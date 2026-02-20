# Project Skeleton & Data Contracts

## Purpose
This document defines the **foundational contracts** of the Experimentation & Causal Inference Platform.
For data scientists, the goal is to ensure that *all downstream logic is boring, predictable, and reproducible*.

Before any statistics are run, we lock down:
- schemas
- configuration surfaces
- synthetic data guarantees
- idempotent execution paths

This mirrors how mature experimentation platforms are built.

---

## Design Principles

1. **Schemas are contracts, not suggestions**
2. **Config drives behavior, not code branches**
3. **Synthetic data must exercise real failure modes**
4. **Every pipeline must be safely rerunnable**

---

## Repository Layout

```
exp_platform/
├── config.py
├── generate_synth.py
├── synth/
├── schemas.py
├── schemas/
│   ├── exposures.schema.json
│   ├── outcomes.schema.json
│   └── experiment_registry.schema.json
├── pipelines/
├── stats/
├── causal/
```

---

## Canonical Datasets

### exposures
Represents assignment + exposure events.

| Field | Description |
|------|-------------|
| experiment_id | Experiment identifier |
| user_id | Stable unit of analysis |
| variant | control / treatment |
| exposure_ts | Exposure timestamp |
| unit | user / session |

### outcomes
Metric observations.

| Field | Description |
|------|-------------|
| experiment_id | Experiment identifier |
| user_id | Unit |
| metric_name | Metric key |
| value | Numeric value |
| outcome_ts | Event timestamp |

### experiment_registry
Source of truth for experiment windows and metadata.

---

## Synthetic Data Guarantees

Synthetic data generation must:
- obey schemas
- produce realistic distributions
- include pre/post windows
- allow controlled effect sizes
- be deterministic given a seed

This ensures pipelines fail *locally* before production.

---

## Definition of Done

- One command generates raw parquet
- Schemas validate ingest
- Config loads cleanly (dev/prod)
- Downstream pipelines rely on contracts only