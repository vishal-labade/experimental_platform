# A/B Inference

## Purpose
Transform metric aggregates into **actionable decisions**.

This layer prioritizes:
- interpretability
- correctness
- explicit uncertainty

---

## Statistical Toolkit

Minimum supported:
- difference in means
- standard errors
- confidence intervals
- relative lift
- guardrail flags

P-values are optional; CIs are mandatory.

---

## Outputs

Written to Iceberg:
- experiment_id
- metric_name
- control_mean
- treatment_mean
- delta
- relative_lift
- ci_low / ci_high
- analysis_ts

---

## Decision Framing

Every result answers:
> What changed, by how much, and how sure are we?

---

## Definition of Done

- One command produces inference
- Memo auto-populates metrics
- Guardrails are explicit