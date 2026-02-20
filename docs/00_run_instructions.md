# End-to-End Experiment Run Guide

This document describes how to run the **Experimentation & Causal Inference Platform**
end-to-end for a *single* `experiment_id` using the platform orchestration entry point.

The goal is to provide a **single, deterministic command** that executes the full
experimentation lifecycle:

> generate → ingest → validate → aggregate → analyze → memo

This workflow is intentionally designed to feel like a **platform entry point**, not a notebook.

---

## What This Script Guarantees

Running the end-to-end pipeline guarantees:

- A single source of truth per `experiment_id`
- Idempotent execution (safe to rerun)
- Ordered, explicit pipeline stages
- Clear failure semantics (required vs optional steps)
- Human-readable artifacts (decision memo)

If the run completes successfully, the experiment is **fully analyzable and review-ready**.

---

## Prerequisites

Before running end-to-end:

- Python environment with `exp_platform` installed
- Spark configured with Iceberg support
- MinIO reachable via `s3a://`
- Config file available (default: `configs/dev.yaml`)

No cloud accounts, external databases, or Kubernetes are required.

---

## Basic Usage

### Minimal Run (Existing Raw Data)

Use this when raw parquet already exists (e.g., previously generated or ingested):

```bash
python run_end_to_end.py   --experiment-id exp_checkout_button_001
```

This will:

1. Ingest raw data into Iceberg
2. Run data quality checks
3. Compute metric aggregates
4. Run A/B inference
5. Run CUPED (if eligible)
6. Run Difference-in-Differences
7. Attempt pre-trend diagnostics
8. Write a decision memo

---

### Full Run with Synthetic Data Generation

Use this for local iteration, demos, or validation:

```bash
python run_end_to_end.py   --experiment-id exp_checkout_button_001   --generate-synth
```

This additionally:

- Generates synthetic raw parquet scoped to `experiment_id`
- Writes raw data to MinIO (default) or local filesystem

Synthetic data generation is deterministic per `(experiment_id, seed)`.

---

## Execution Stages

The end-to-end runner executes the following stages **in order**.

### 0. Synthetic Data Generation (Optional)

```bash
python -m exp_platform.generate_synth   --config configs/dev.yaml   --target minio   --experiment-id <experiment_id>
```

- Enabled via `--generate-synth`
- Uses YAML-defined scenarios
- Writes raw parquet to:

```
s3://raw/exp_platform/<experiment_id>/
```

---

### 1. Ingest Raw Data → Iceberg (Required)

```bash
python -m exp_platform.pipelines.run_ingest   --config configs/dev.yaml   --experiment-id <experiment_id>
```

- Validates schemas
- Performs idempotent writes
- Populates canonical Iceberg tables

Failure at this stage is **fatal**.

---

### 2. Data Quality Report (Required by Default)

```bash
python -m exp_platform.pipelines.dq_report   --experiment-id <experiment_id>
```

Checks include:
- Missing keys
- Duplicate rows
- Time-window sanity
- Row-count validation

Disable via:

```
--skip-dq
```

---

### 3. Metric Aggregation

```bash
python -m exp_platform.pipelines.compute_metric_aggregates   --experiment-id <experiment_id>
```

Produces:
- `metric_aggregates_overall`
- `metric_aggregates_daily`

Disable via:

```
--skip-metrics
```

---

### 4. A/B Inference

```bash
python -m exp_platform.pipelines.analyze_experiment   --experiment-id <experiment_id>
```

Computes:
- Control vs treatment means
- Absolute and relative lift
- Confidence intervals
- Guardrail flags

Disable via:

```
--skip-ab
```

---

### 5. CUPED (Variance Reduction)

```bash
python -m exp_platform.pipelines.analyze_experiment_cuped   --experiment-id <experiment_id>
```

- Applies CUPED when eligible
- Reports both raw and adjusted estimates
- Records variance reduction

Disable via:

```
--skip-cuped
```

---

### 6. Difference-in-Differences (DiD)

```bash
python -m exp_platform.pipelines.analyze_experiment_did   --experiment-id <experiment_id>
```

Used when:
- Randomization is imperfect
- Staged rollouts or policy changes are present

Disable via:

```
--skip-did
```

---

### 7. Pre-Trend Diagnostics (Best-Effort)

```bash
python -m exp_platform.pipelines.did_pretrends   --experiment-id <experiment_id>   --metric-pre pre_revenue
```

- Optional diagnostic
- May return `INSUFFICIENT_DATA`
- Failure does **not** fail the overall run

Disable via:

```
--skip-pretrends
```

---

## Full End-to-End One-Liner

Ensure the experiment is registered in `configs/dev.yaml`, for example:

```yaml
experiments:
  exp_add_cart_002:
    seed: 51
    n_users: 40000
    treatment_share: 0.5
    effect_lift_conversion: 0.02
    revenue_mean: 12.0
    revenue_sd: 3.0
```

### Execution One-Liner

```bash
python -m exp_platform.pipelines.run_end_to_end   --config configs/dev.yaml   --experiment-id exp_pricing_test_003   --generate-synth
```

### What This Command Does

This single command executes the **entire experimentation lifecycle** for
`exp_pricing_test_003`:

1. Generates synthetic raw data
2. Ingests data into Iceberg (`exposures`, `outcomes`, `experiment_registry`)
3. Runs data quality checks
4. Computes metric aggregates
5. Runs A/B inference
6. Runs CUPED variance reduction (if eligible)
7. Runs Difference-in-Differences
8. Runs pre-trend diagnostics (best-effort)
9. Writes a decision memo

Decision memo location:

```
data/memos/exp_pricing_test_003.md
```

---

## Optional Execution Controls

### Dry Run

Prints all commands without executing:

```bash
python run_end_to_end.py   --experiment-id exp_checkout_button_001   --dry-run
```

### Custom Python Binary

```bash
python run_end_to_end.py   --experiment-id exp_checkout_button_001   --python /path/to/python
```

---

## Failure Semantics

- **Required stages** fail fast and stop execution
- **Optional stages** log warnings and continue
- All failures are explicit and surfaced

This makes partial results inspectable without ambiguity.

---

## Outputs

### Iceberg Tables

Key outputs include:

- `iceberg.exp.exposures`
- `iceberg.exp.outcomes`
- `iceberg.exp.metric_aggregates_*`
- `iceberg.exp.analysis_results`
- `iceberg.exp.analysis_results_cuped`
- `iceberg.exp.did_results`
- `iceberg.exp.did_pretrend_*`


**Sample Query:**

Here is sample query to check that experiment was registered:

```
python - <<'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("check_dupes").getOrCreate()

exp_id = "exp_pricing_test_003"

spark.sql(f"""
SELECT *
FROM exp.exp.experiment_registry
WHERE experiment_id = '{exp_id}'
""").show(truncate=False)
EOF

```

---

### Decision Memo

Each run produces a Markdown memo:

```
data/memos/<experiment_id>.md
```

The memo summarizes:
- Key metric movements
- Statistical evidence
- Causal validity notes
- Recommended decision

---

## Idempotency & Reruns

Re-running the pipeline with the same `experiment_id`:

- Replaces experiment-scoped data
- Preserves other experiments
- Produces deterministic outputs

This behavior is intentional and required for iteration.

---

## Definition of Success

An end-to-end run is successful when:

- All required stages complete
- Iceberg tables are populated
- A decision memo is written
- Results are reproducible by `experiment_id`

If all conditions hold, the experiment is **review-ready**.