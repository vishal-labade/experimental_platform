# Metrics Layer

## Purpose
The metrics layer converts raw events into **decision-grade aggregates**.

For Data scientists, metrics are treated as **products**:

- centrally defined
- versioned
- reproducible

---

## Metric Definition Model

All metrics live in:
`exp_platform/metrics/definitions.py`

Each metric defines:
- aggregation grain
- numerator / denominator
- guardrail behavior

---

## Aggregation Guarantees

- User-level aggregation first
- Variant-level aggregation second
- Fixed experiment windows
- Deterministic grouping

Outputs:
- metric_aggregates_daily
- metric_aggregates_overall

---

## Guardrails

Metrics can be marked as:
- primary
- secondary
- guardrail

Guardrails surface **risk**, not success.

---

## Definition of Done

- Adding a metric requires one edit
- Aggregates rerun cleanly
- Windowing is consistent