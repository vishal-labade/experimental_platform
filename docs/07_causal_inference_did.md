# Difference-in-Differences (DiD)

## Purpose
Handle cases where randomization is unavailable or incomplete.

This models real product constraints.

---

## Estimation

Cell means:
(control/treatment × pre/post)

Produces:
- DiD estimate
- SE / CI
- p-value (optional)

---

## Pre-Trend Diagnostics

Checks:
- delta_k trends
- slope stability
- data sufficiency

INSUFFICIENT_DATA is a **valid outcome**, not failure.

---

## Outputs

- did_results
- did_pretrend_delta
- did_pretrend_tests

---

## Definition of Done

- Assumptions are checked
- Limitations are recorded
- Memo explains validity