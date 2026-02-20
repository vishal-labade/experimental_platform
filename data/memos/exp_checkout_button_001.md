# Experiment Decision Memo (Draft)

**Experiment:** `exp_checkout_button_001`

**Confidence:** 0.95

**Data source:** `iceberg.exp.metric_aggregates_overall` → `iceberg.exp.analysis_results`


---

## Summary Table

| metric | control mean | treatment mean | delta | rel lift | CI low | CI high | p-value |
|---|---:|---:|---:|---:|---:|---:|---:|
| conversion | 0.100574 | 0.122878 | 0.0223037 | 0.221764 | 0.0145215 | 0.030086 | 1.94092e-08 |
| pre_revenue | 12.4813 | 12.5169 | 0.035636 | 0.00285517 | -0.0376998 | 0.108972 | 0.340892 |
| revenue | 1.25174 | 1.54759 | 0.295844 | 0.236346 | 0.195378 | 0.396311 | 7.85657e-09 |

---

## Notes

- Method: two-sample difference in means with normal approximation.

- This memo is auto-generated; add product context + risks before sharing.

---
## CUPED (Variance Reduction)

- Covariate: `pre_revenue`

- Target metric: `revenue`

- Theta: `0.00433802`

- SE gain: `0.00%` (positive means tighter)

- CI width gain: `0.00%` (positive means tighter)

---
## DiD Pre-trend Diagnostics (Chunk 6.2)

- Metric: `pre_revenue`

- Grain: `day`

- Window: pre `28`, post `28` buckets (relative to start)

- Method: pre-slope test on `delta_k = treat_k - ctrl_k` for `k<0`

- num_pre_points: `14`

- slope: `0.005271134731843119`  se: `0.010334394389266026`  p: `0.6100112218226055`

- Verdict: `PASS`

---
## DiD Pre-trend Diagnostics
- Metric: `pre_revenue`
- Grain: `day`
- num_pre_points: `14`
- slope: `0.005271134731843616`  se: `0.010334394389266088`  p: `0.610011221822574`
- Verdict: `PASS`


## CUPED
- target=revenue
- covariate=pre_revenue
- theta=0.00433802, x_mean=12.4991
- delta=0.290012, p=1.33104e-08, ci95=[0.189975,0.390049]


## Pretrends check (DiD assumption)
- metric_pre: `pre_revenue`
- inferred event_ts (min exposure_ts): `2026-01-01 00:00:27`
- outcomes time column used: `ts`
- pre window: `14` days
- slope(treatment-control) over pre days: `0.00535486`
- se: `0.0109141`  z: `0.490639`  p: `0.623682`
- verdict: **PASS** (p>0.05 => no evidence of pretrend)

---
## DiD Pre-trend Diagnostics (Chunk 6.2)

- Metric: `revenue`

- Grain: `day`

- Window start: `2026-01-01 00:00:27`

- Window: pre `14`, post `14` buckets (relative to start)

- Method: pre-slope test on `delta_k = treat_k - ctrl_k` for `k<0`

- num_pre_points: `0`

- slope: `nan`  se: `nan`  p: `nan`

- Verdict: `INSUFFICIENT_DATA`
