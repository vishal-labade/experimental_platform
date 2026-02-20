# CUPED & Variance Reduction

## Purpose
Improve experiment sensitivity **without increasing sample size**.

CUPED demonstrates experimentation maturity.

---

## Method

For eligible metrics:
Y_adj = Y − θ(X − mean(X))

Where:
- X is pre-period covariate
- θ minimizes variance

---

## Reporting

Always report:
- raw estimate
- CUPED-adjusted estimate
- variance reduction %

Never hide raw results.

---

## Governance Note

If covariates are weak or unavailable:
- CUPED is skipped
- Limitation is documented
- No silent adjustments

---

## Definition of Done

- Variance reduction is measurable
- Outputs are transparent
- Limitations are explicit