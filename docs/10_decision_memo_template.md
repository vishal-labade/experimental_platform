# Experiment Decision Memo

> **Audience:** Product Leadership, Engineering, Data Science  
> **Goal:** Decide whether to ship, rollback, iterate, or run follow-up experiments.

---

## 1. Executive Summary (TL;DR)

**Experiment:** {{experiment_id}}  
**Primary Metric:** {{primary_metric}}  
**Decision:** {{SHIP / HOLD / ITERATE / ROLLBACK}}  

**Observed Effect:**  
- Absolute lift: {{delta}}  
- Relative lift: {{relative_lift}}  
- 95% CI: {{ci_low}} to {{ci_high}}

**Confidence:** {{High / Medium / Low}}  
**Key Risk:** {{main_risk_or_guardrail}}

---

## 2. Experiment Context

**Hypothesis**  
> {{Clear, falsifiable statement}}

**Variants**
- Control: {{description}}
- Treatment: {{description}}

**Population**
- Units: {{user / session / account}}
- Sample size: {{n_control}} / {{n_treatment}}
- Duration: {{start_ts}} → {{end_ts}}

---

## 3. Results Overview

### Primary Metric

| Variant | Mean | Delta vs Control |
|------|------|------------------|
| Control | {{mean_c}} | — |
| Treatment | {{mean_t}} | {{delta}} |

### Guardrails

| Metric | Status | Notes |
|------|------|------|
| {{guardrail_1}} | PASS / FAIL | {{note}} |
| {{guardrail_2}} | PASS / FAIL | {{note}} |

---

## 4. Statistical Evidence

- Method: {{A/B | CUPED | DiD}}
- Estimator assumptions checked: {{Yes / Partial / No}}
- Precision improvements (if CUPED): {{variance_reduction_pct}}%

**Interpretation**  
> {{Plain-English explanation of uncertainty}}

---

## 5. Causal Validity & Risks

- Randomization quality: {{Good / Degraded / N/A}}
- Pre-trends (if DiD): {{PASS / FAIL / INSUFFICIENT_DATA}}
- Known confounders: {{list or none}}

---

## 6. Recommendation

**Recommended Action**
- {{Ship / Iterate / Rollback / Run Follow-up}}

**Rationale**
- {{Bullet points tying data → decision}}

**Follow-ups**
- {{Additional experiment / monitoring / metric to add}}

---

## 7. Appendix (Optional)

- Iceberg tables: `iceberg.exp.*`
- Analysis timestamp: {{analysis_ts}}
- Reproducibility: rerunnable by `experiment_id`