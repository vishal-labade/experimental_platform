# Experiment Scenario Behavior Report

## Duration & Sample Size Effects in A/B Testing

------------------------------------------------------------------------

# 1. Purpose

This document focuses exclusively on how the experimentation platform
behaves under different experimental regimes.

We analyze:

-   Short vs Long duration experiments
-   Small vs Large sample size experiments

CUPED and DiD are intentionally excluded here to isolate raw A/B
behavior.

------------------------------------------------------------------------

# 2. Short vs Long Duration Experiments

Comparison: - Short Duration: 3 Days
- Long Duration: 28 Days
- Same parameters, same inference engine

------------------------------------------------------------------------

## 2.1 Conversion (North Star Metric)

### Short (3 Days)

-   Delta: 0.0524327
-   CI: [0.0462192, 0.0586462]
-   CI Width ≈ 0.01243

### Long (28 Days)

-   Delta: 0.0491335
-   CI: [0.042895, 0.0553721]
-   CI Width ≈ 0.01248

### Interpretation

1.  **Effect Stabilization** The longer experiment produces a slightly
    lower lift estimate. This suggests that short-duration experiments
    may capture novelty or transient early effects.

2.  **Precision Behavior** Confidence interval width remains nearly
    identical. Duration alone does not guarantee improved statistical
    precision. Precision depends on effective exposed sample size and
    variance, not calendar time.

------------------------------------------------------------------------

## 2.2 Revenue (Guardrail Metric)

### Short

-   Delta: 0.657079
-   CI Width ≈ 0.1607

### Long

-   Delta: 0.594383
-   CI Width ≈ 0.1613

### Interpretation

1.  Longer duration moderates the effect size slightly.
2.  Confidence interval width remains stable.
3.  Calendar duration does not automatically increase statistical power
    unless effective sample size scales.

------------------------------------------------------------------------

## Key Principle (Duration)

Duration affects **stability of estimates**, not necessarily
**precision**.

Longer experiments can reduce short-term inflation effects but will only
improve precision if they increase effective exposure volume or reduce
variance.

------------------------------------------------------------------------

# 3. Small vs Large Sample Size Experiments

Comparison: - Small Sample: 10,000 users
- Large Sample: 200,000 users
- Same duration and parameters

------------------------------------------------------------------------

## 3.1 Conversion Metric

### Small (10k)

-   Delta: 0.0596667
-   CI: [0.0457018, 0.0736316]
-   CI Width ≈ 0.02793

### Large (200k)

-   Delta: 0.0499485
-   CI: [0.0468032, 0.0530937]
-   CI Width ≈ 0.00629

Observed CI shrinkage:

`0.02793 / 0.00629 ≈ 4.44×`

Theoretical expectation:

$$ \sqrt(200000 / 10000) = \sqrt(20) ≈ 4.47× $$

Result: Observed precision scaling matches theoretical √n behavior.

------------------------------------------------------------------------

## 3.2 Revenue Metric

### Small Sample

-   CI Width ≈ 0.36645

### Large Sample

-   CI Width ≈ 0.08111

Observed shrinkage ≈ 4.52×

Again aligns with √n scaling.

------------------------------------------------------------------------

## Interpretation

1.  **Precision scales with √n.**
2.  Larger samples dramatically reduce uncertainty.
3.  Small samples produce noisier and more extreme point estimates.
4.  Larger samples stabilize toward the true underlying effect.

------------------------------------------------------------------------

# 4. Combined Insights

  -------------------------------------------------------------------------
  Scenario       Effect Stability         Precision       Key Driver
  -------------- ------------------------ --------------- -----------------
  Short Duration Slightly inflated        Similar         Novelty / early
                                                          response

  Long Duration  Stabilized               Similar         Same effective N

  Small Sample   Noisy                    Wide CI         High variance

  Large Sample   Stable                   Narrow CI       √n scaling
  -------------------------------------------------------------------------

------------------------------------------------------------------------

# 5. Final Takeaways

1.  Increasing duration does not automatically increase precision.
2.  Increasing sample size reliably improves precision via √n scaling.
3.  Short experiments may overestimate lift due to early behavioral
    effects.
4.  Large experiments produce more stable and economically interpretable
    estimates.
5.  The platform's inference engine behaves consistently with
    statistical theory across all regimes.

This confirms both the statistical correctness and realistic behavioral
dynamics of the experimentation platform.