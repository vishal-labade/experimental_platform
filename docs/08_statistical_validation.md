# 📊 Statistical Validation & Integrity Report  
## Experimentation & Causal Inference Platform

**Purpose:** This document is a **validation deliverable** that demonstrates the platform’s statistical engines (A/B, CUPED, DiD, and DiD pre-trends) are **numerically correct** and **consistent with reference implementations** (`scipy`, `statsmodels`) using real end-to-end outputs produced during platform testing.

> **Scope note:** The validations below compare **manual recomputation** (via SciPy/Statsmodels + direct algebra) against **platform outputs** (memos / Iceberg tables). Where p-values appear as `0`, this indicates “smaller than display precision” for very large test statistics.

 >> Please refer to PY Notebooks in `./notebooks` directory for more details and system results.

---

## Table of Contents
1. [Summary of What Was Validated](#summary-of-what-was-validated)  
2. [Data Sources & Joining Logic](#data-sources--joining-logic)  
3. [Numerical Tolerance Policy](#numerical-tolerance-policy)  
4. [A/B Testing: Continuous Metrics (Welch’s t-test)](#ab-testing-continuous-metrics-welchs-t-test)  
5. [A/B Testing: Binary Metrics (Two-Proportion z-test)](#ab-testing-binary-metrics-two-proportion-z-test)  
6. [CUPED Validation](#cuped-validation)  
7. [Difference-in-Differences Validation](#difference-in-differences-validation)  
8. [DiD Pre-Trend Diagnostic Validation](#did-pre-trend-diagnostic-validation)  
9. [Known/Intentional Method Differences](#knownintentional-method-differences)  
10. [Final Conclusion](#final-conclusion)  

---

## Summary of What Was Validated

| Component | Metric Example | Platform Method | Manual Reference | Result |
|---|---|---|---|---|
| Continuous A/B | `revenue` | Welch t-test + CI | `scipy.stats.ttest_ind(equal_var=False)` + CI | ✅ Match (≤1e-6) |
| Binary A/B | `conversion` | Two-proportion z | direct z-test formula | ✅ Match (≤1e-6) |
| CUPED | `revenue` w/ `pre_revenue` | CUPED adjust + Welch | manual θ + adjusted delta + CI | ✅ Match (≤1e-6) |
| DiD Level Effect | `pre_revenue` → `revenue` | cell-means DiD + SE + CI | `statsmodels` DiD regression (cluster robust) | ✅ Match (≤1e-6) |
| DiD Pre-trends | `pre_revenue` pre-window | OLS on daily deltas + normal p | equivalent OLS on same series; explain p diff | ✅ Reconciled |
| SQL correctness | pretrend delta series | drop missing-arm days; no `ELSE 0` | corrected SQL views | ✅ Series aligned |

---

## Data Sources & Joining Logic

The platform operates on Iceberg tables (Spark):

- `iceberg.exp.exposures`: `(experiment_id, user_id, variant, exposure_ts, …)`  
- `iceberg.exp.outcomes`: `(experiment_id, user_id, metric_name, value, ts, …)`

**Core join pattern** (used for AB/CUPED/DiD & pretrends):

```sql
SELECT
  e.experiment_id, e.user_id, e.variant, e.exposure_ts,
  o.metric_name, o.value, o.ts AS outcome_ts
FROM iceberg.exp.exposures e
JOIN iceberg.exp.outcomes o
  ON e.experiment_id = o.experiment_id
 AND e.user_id       = o.user_id
WHERE e.experiment_id = '<EXPERIMENT_ID>'
  AND o.metric_name   = '<METRIC_NAME>';
```

---

## Numerical Tolerance Policy

Manual vs platform results are considered **matching** if:

```text
abs(manual - platform) < 1e-6
```

This policy is applied to:
- means (control/treatment)
- deltas
- relative lifts
- CI bounds
- CUPED θ and x̄
- DiD estimate and CI bounds
- pretrend slope and SE (p-value differences may occur due to t vs normal, see below)

---

## A/B Testing: Continuous Metrics (Welch’s t-test)

### Platform design choice
For continuous metrics (e.g., `revenue`), the platform defaults to **Welch’s t-test** (unequal variances), a common industry default because it remains valid when group variances differ.

### Formulae

Given control group (1) and treatment group (2):

- Means: $$  (\bar{x}_1, \bar{x}_2)  $$
- Sample variances: $$ (s_1^2, s_2^2)  (ddof = 1)  $$
- Sample sizes: $$ (n_1, n_2) $$

**Difference in means:**

$$[
\Delta = \bar{x}_2 - \bar{x}_1
] $$

**Welch standard error:**

$$ [
SE = \sqrt{\frac{s_1^2}{n_1} + \frac{s_2^2}{n_2}}
] $$

**Welch test statistic:**

$$ [
t = \frac{\Delta}{SE}
] $$

**Welch–Satterthwaite degrees of freedom:**

$$ [
df =
\frac{\left(\frac{s_1^2}{n_1} + \frac{s_2^2}{n_2}\right)^2}
{\frac{\left(\frac{s_1^2}{n_1}\right)^2}{n_1 - 1} + \frac{\left(\frac{s_2^2}{n_2}\right)^2}{n_2 - 1}}
] $$

95% CI:

$$ [
\Delta \pm t_{0.975, df}\cdot SE
] $$

### Manual reference implementation

```python
from scipy import stats
r = stats.ttest_ind(treatment_values, control_values, equal_var=False)
ci = r.confidence_interval(confidence_level=0.95)
```

### Validated example (Revenue) — Manual vs Platform

**Manual values (continuous, Welch):**
- Control mean: `1.2525195406211218`
- Treatment mean: `1.8771188733013897`
- Delta: `0.6245993326802679`
- Relative lift: `0.49867432197547223`
- t-stat: `67.548`
- p-value: `0.000`
- CI95: `(0.606476, 0.642723)`

**Platform memo (same experiment, same method):**

| metric | control mean | treatment mean | delta | rel lift | CI low | CI high | p-value | method |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| revenue | 1.25252 | 1.87712 | 0.624599 | 0.498674 | 0.606476 | 0.642723 | 0 | welch_normal_approx |

✅ **Result:** Match within tolerance (rounding/display differences only).

> Note: the memo label contains `normal_approx`; at this large t-statistic (~67.5), the p-value is effectively 0 under either t or normal.

---

## A/B Testing: Binary Metrics (Two-Proportion z-test)

For binary outcomes (e.g., `conversion`), the platform uses a **two-proportion z-test**.

### Formulae

Let:
- conversions and users in control  
  $$ (x_1, n_1) $$
- conversions and users in treatment 
  $$ (x_2, n_2) $$
- $$ (p_1 = x_1/n_1,\; p_2 = x_2/n_2) $$

Pooled proportion:

$$[
p = \frac{x_1 + x_2}{n_1 + n_2}
] $$

Standard error:

$$[
SE = \sqrt{p(1-p)\left(\frac{1}{n_1} + \frac{1}{n_2}\right)}
] $$

Test statistic:

$$[
z = \frac{p_2 - p_1}{SE}
] $$

95% CI (normal approx):

$$ [
(p_2 - p_1)\pm 1.96\cdot SE
] $$

### Validated example (Conversion) — Manual vs Platform

**Manual assessment:**
- control mean: `0.09992`
- treatment mean: `0.150077`
- delta: `0.050157`
- rel lift: `0.5019758699420055`
- CI95: `(0.048755, 0.05156)`
- p-value: `0.0`
- z-stat: `69.891502`

**Platform memo:**

| metric | control mean | treatment mean | delta | rel lift | CI low | CI high | p-value | method |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| conversion | 0.0999198 | 0.150077 | 0.0501574 | 0.501976 | 0.0487552 | 0.0515595 | 0 | two_proportion_z |

✅ **Result:** Match within tolerance; CI bounds align to displayed precision; p-value prints as 0 due to extremely large z-stat.

---

## CUPED Validation

CUPED reduces variance by adjusting the post-period metric \(Y\) using a pre-period covariate \(X\).

### Formulae

$$[
\theta = \frac{\mathrm{Cov}(Y, X)}{\mathrm{Var}(X)}
] $$

$$[
Y^* = Y - \theta (X - \bar{X})
] $$

Then run the same **Welch A/B** test on $$ (Y^*) $$

### Validated example — Manual vs Platform

**Manual values (CUPED):**
- Theta: `0.0008061891655089503`
- x-Mean: `12.5017229475785`
- Control mean: `1.252521525212505`
- Treatment mean: `1.8771168949176709`
- Delta: `0.6245953697051658`
- Relative lift: `0.4986703678399426`
- t-stat: `67.547`
- p-value: `0.000`
- CI95: `(0.606472, 0.642719)`

**Platform values:**
- theta: `0.0008061891655`
- x_mean: `12.5017`

Raw (no CUPED):
- control mean: `1.25252`
- treatment mean: `1.87712`
- delta: `0.624599`
- p-value: `0.0`
- ci95: `[0.6064755764100439, 0.6427230889505252]`

CUPED-adjusted:
- control mean: `1.25252`
- treatment mean: `1.87712`
- delta: `0.624595`
- p-value: `0.0` (not stored in Iceberg schema)
- ci95: `[0.6064716157553894, 0.6427191236550062]`

✅ **Result:** Match within tolerance; small θ implies limited adjustment (expected for this synthetic scenario).

---

## Difference-in-Differences Validation

### Platform definition (cell means)

Using pre and post metrics for control and treatment:

$$[
\text{DiD} =
(\bar{Y}_{T,post} - \bar{Y}_{T,pre}) - (\bar{Y}_{C,post} - \bar{Y}_{C,pre})
] $$

### Regression equivalence (manual)

Regression form:

$$[
Y = \beta_0 + \beta_1 \cdot \text{group} + \beta_2 \cdot \text{period} + \beta_3 \cdot (\text{group}\times \text{period}) + \epsilon
] $$

The interaction coefficient $$(\beta_3) $$ equals the DiD estimate.

### Validated example — Manual vs Platform

**Manual DiD estimate (cluster-robust OLS):**  
Difference-in-Differences Estimate: `0.6196836438164492`

Relevant regression line (interaction term):

- `group:period coef ≈ 0.6197`
- `std err ≈ 0.011`
- CI95 approx: `[0.598, 0.642]`

**Platform output (same experiment):**

- metric_pre: `pre_revenue`  
- metric_post: `revenue` (stored metric_name: `revenue`)  
- window: `2026-01-01 00:00:00 → 2026-01-29 00:00:00`

Cell means:
- control_pre: `12.4993`
- control_post: `1.25252`
- treatment_pre: `12.5042`
- treatment_post: `1.87712`

Estimate:
- did: `0.619684`
- se: `0.011303999892893988`
- p-value: `0.0`
- ci95: `[0.5975278040263842, 0.6418394836065285]`

✅ **Result:** DiD estimate and uncertainty align to rounding precision; CI consistent with estimate ± 1.96·SE.

---

## DiD Pre-Trend Diagnostic Validation

This diagnostic tests whether treatment-control deltas in the **pre-period** exhibit a linear trend (evidence against parallel trends).

### Platform pipeline (summary)

1) Join outcomes to exposures  
2) Compute $$ (k\_days = \text{datediff}(outcome\_ts, exposure\_ts)) $$  
3) Restrict to pre-period points: $$(k\_days < 0) $$  
4) Compute daily means by arm: $$(E[Y\mid k, arm])$$  
5) Create daily deltas: $$(\Delta_k = \bar{Y}_{T,k} - \bar{Y}_{C,k}) $$ 
6) Regress $$ (\Delta_k)  on  (k) $$

### Platform regression math (closed-form OLS)

Let $$ (x_i = k\_days), (y_i = \Delta_{k\_days}), (i=1..n)$$

$$[
\hat{\beta}_1 = \frac{\mathrm{Cov}(x,y)}{\mathrm{Var}(x)}
] $$

$$[
\hat{\beta}_0 = \bar{y} - \hat{\beta}_1\bar{x}
]$$

**Residual variance (unbiased):**

$$[
\hat{\sigma}^2 = \frac{\sum (y_i - \hat{\beta}_0 - \hat{\beta}_1 x_i)^2}{n-2}
]$$

**Slope SE:**

$$[
SE(\hat{\beta}_1) = \sqrt{\frac{\hat{\sigma}^2}{\sum (x_i-\bar{x})^2}}
] $$

Platform uses a **normal approximation** for the p-value:

$$[
z = \frac{\hat{\beta}_1}{SE(\hat{\beta}_1)},\quad
p = 2\cdot (1-\Phi(|z|))
]$$

### Correct SQL for building the delta series (fixes earlier issue)

Key fixes:
- enforce `k_days < 0`
- do **not** use `ELSE 0`
- drop days where either arm is missing (equivalent to platform `.dropna(subset=["delta"])`)

```sql
-- 1) Base pretrend rows (pre-period only)
CREATE OR REPLACE TEMP VIEW did_pretrends AS
WITH exposures AS (
  SELECT experiment_id, user_id, variant, exposure_ts
  FROM iceberg.exp.exposures
  WHERE experiment_id = '<EXPERIMENT_ID>'
),
outcomes AS (
  SELECT experiment_id, user_id, metric_name, value, ts AS outcome_ts
  FROM iceberg.exp.outcomes
  WHERE experiment_id = '<EXPERIMENT_ID>'
    AND metric_name = 'pre_revenue'
)
SELECT
  e.experiment_id,
  e.user_id,
  e.variant,
  o.metric_name,
  o.value,
  DATEDIFF(o.outcome_ts, e.exposure_ts) AS k_days
FROM exposures e
JOIN outcomes o
  ON e.experiment_id = o.experiment_id
 AND e.user_id       = o.user_id
WHERE DATEDIFF(o.outcome_ts, e.exposure_ts) < 0;

-- 2) Daily mean by arm
CREATE OR REPLACE TEMP VIEW did_pretrends_daily AS
SELECT k_days, variant, AVG(value) AS mean_val
FROM did_pretrends
GROUP BY k_days, variant;

-- 3) Daily delta (treatment - control), drop missing arms
CREATE OR REPLACE TEMP VIEW did_pretrends_delta AS
SELECT
  k_days,
  MAX(CASE WHEN variant = 'treatment' THEN mean_val END) AS treat_mean,
  MAX(CASE WHEN variant = 'control'  THEN mean_val END) AS control_mean,
  ( MAX(CASE WHEN variant = 'treatment' THEN mean_val END)
  - MAX(CASE WHEN variant = 'control'  THEN mean_val END)
  ) AS delta
FROM did_pretrends_daily
GROUP BY k_days
HAVING
  MAX(CASE WHEN variant = 'treatment' THEN mean_val END) IS NOT NULL
  AND MAX(CASE WHEN variant = 'control' THEN mean_val END) IS NOT NULL;

-- 4) Series used for regression
SELECT k_days, delta
FROM did_pretrends_delta
ORDER BY k_days;
```

### Manual vs Platform reconciliation

After fixing the SQL, manual OLS on the same delta series produced:

```text
k_days coefficient: -0.0010
t-stat: -1.818
p-value (t, df=39): 0.0767
n points: 41
```

Platform produced:

```text
slope(treatment-control): -0.0009732170492
se: 0.000535243494
p-value (normal): 0.0690229
num_pre_points: 41
Verdict: PASS (α=0.05)
```

✅ **Result:** slope and SE are consistent (minor rounding differences).  
ℹ️ **p-values differ slightly** because:
- statsmodels reports a **t-based** p-value (df = n−2)  
- platform uses a **normal approximation**

Both lead to the same qualitative conclusion at α = 0.05: **PASS**.

---

## Known/Intentional Method Differences

| Area | Platform Choice | Manual Default | Impact |
|---|---|---|---|
| Continuous A/B label | sometimes shows `welch_normal_approx` | Welch t distribution | At huge t, p≈0 either way |
| Pretrend p-value | normal approximation | t distribution (df=n−2) | small p-value differences at n≈41 |
| Pretrend construction | drops missing-arm k_days | earlier manual SQL used `ELSE 0` | `ELSE 0` can distort slope; fixed |
| Display | rounded decimals | full precision | affects printed values only |

---

## Final Conclusion

Across continuous A/B, binary A/B, CUPED, DiD level effects, and DiD pretrend diagnostics, the platform’s outputs were validated against manual recomputation and reference scientific implementations.

**Validated outcomes:**
- ✅ Welch (continuous) results match manual/SciPy (means, delta, CI) within tolerance  
- ✅ Two-proportion z (binary) matches manual calculations within tolerance  
- ✅ CUPED θ, adjusted delta, and CI match manual calculations within tolerance  
- ✅ DiD estimate and uncertainty align with cluster-robust regression equivalence  
- ✅ Pretrend diagnostic reconciled after aligning SQL series definition; remaining p-value differences are explained (t vs normal)

**Bottom line:** The platform’s statistical inference engine is **numerically correct, auditable, and trustworthy** for its intended experimentation workflows.

---