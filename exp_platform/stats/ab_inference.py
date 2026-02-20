from __future__ import annotations

from dataclasses import dataclass, asdict
from math import erf, sqrt
from typing import Dict, Any, Iterable, Optional, List


def _norm_cdf(x: float) -> float:
    # Standard normal CDF using erf (no scipy dependency)
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def _two_sided_p_from_z(z: float) -> float:
    # two-sided p-value for normal approximation
    p_one = 1.0 - _norm_cdf(abs(z))
    return 2.0 * p_one


@dataclass
class ABResult:
    metric_name: str
    control_n: int
    treatment_n: int
    control_mean: float
    treatment_mean: float
    absolute_lift: float
    relative_lift: Optional[float]
    stderr: float
    z: float
    p_value: float
    ci95_low: float
    ci95_high: float
    method: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _welch_from_stats(
    metric_name: str,
    n_c: int,
    mean_c: float,
    var_c: float,
    n_t: int,
    mean_t: float,
    var_t: float,
) -> ABResult:
    # Welch-style standard error (normal approximation)
    # stderr = sqrt(var_c/n_c + var_t/n_t)
    se = sqrt((var_c / max(n_c, 1)) + (var_t / max(n_t, 1)))
    diff = mean_t - mean_c

    # Guard: avoid div-by-zero
    z = diff / se if se > 0 else 0.0
    p = _two_sided_p_from_z(z)

    ci_low = diff - 1.96 * se
    ci_high = diff + 1.96 * se

    rel = (diff / mean_c) if mean_c not in (0.0, None) else None

    return ABResult(
        metric_name=metric_name,
        control_n=int(n_c),
        treatment_n=int(n_t),
        control_mean=float(mean_c),
        treatment_mean=float(mean_t),
        absolute_lift=float(diff),
        relative_lift=float(rel) if rel is not None else None,
        stderr=float(se),
        z=float(z),
        p_value=float(p),
        ci95_low=float(ci_low),
        ci95_high=float(ci_high),
        method="welch_normal_approx",
    )


def _prop_z_from_rates(
    metric_name: str,
    n_c: int,
    p_c: float,
    n_t: int,
    p_t: float,
) -> ABResult:
    # Two-proportion z-test (pooled)
    # pooled p = (x_c + x_t) / (n_c + n_t)
    x_c = p_c * n_c
    x_t = p_t * n_t
    denom_n = n_c + n_t
    p_pool = (x_c + x_t) / denom_n if denom_n > 0 else 0.0

    se = sqrt(max(p_pool * (1.0 - p_pool) * (1.0 / max(n_c, 1) + 1.0 / max(n_t, 1)), 0.0))
    diff = p_t - p_c
    z = diff / se if se > 0 else 0.0
    p = _two_sided_p_from_z(z)

    # CI using unpooled standard error (common in reporting)
    se_unpooled = sqrt(
        max(p_c * (1.0 - p_c) / max(n_c, 1) + p_t * (1.0 - p_t) / max(n_t, 1), 0.0)
    )
    ci_low = diff - 1.96 * se_unpooled
    ci_high = diff + 1.96 * se_unpooled

    rel = (diff / p_c) if p_c not in (0.0, None) else None

    return ABResult(
        metric_name=metric_name,
        control_n=int(n_c),
        treatment_n=int(n_t),
        control_mean=float(p_c),
        treatment_mean=float(p_t),
        absolute_lift=float(diff),
        relative_lift=float(rel) if rel is not None else None,
        stderr=float(se_unpooled),
        z=float(z),
        p_value=float(p),
        ci95_low=float(ci_low),
        ci95_high=float(ci_high),
        method="two_proportion_z",
    )


def ab_from_aggregates(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Stable public API expected by pipelines.

    Input: iterable of dict rows with at least:
      - metric_name
      - variant in {"control","treatment"}
      - n
      - mean
      - var (optional; if missing and metric is conversion-like, we use p*(1-p))
    Output: list of dict results (one per metric_name)
    """
    by_metric: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for r in rows:
        metric = str(r["metric_name"])
        variant = str(r["variant"])
        by_metric.setdefault(metric, {})
        by_metric[metric][variant] = r

    out: List[Dict[str, Any]] = []
    for metric, vmap in by_metric.items():
        if "control" not in vmap or "treatment" not in vmap:
            continue

        c = vmap["control"]
        t = vmap["treatment"]

        n_c = int(c["n"])
        n_t = int(t["n"])
        mean_c = float(c["mean"])
        mean_t = float(t["mean"])

        # Decide method
        is_binaryish = metric.lower() in {"conversion", "converted", "purchase", "signup"}

        if is_binaryish:
            res = _prop_z_from_rates(metric, n_c, mean_c, n_t, mean_t)
            out.append(res.to_dict())
            continue

        # Continuous: need variance; if missing, fall back to a conservative assumption
        var_c = c.get("var", None)
        var_t = t.get("var", None)

        if var_c is None or var_t is None:
            # Conservative fallback: treat as Bernoulli with p=clipped mean (works if metric is 0/1-like)
            # If mean is not in [0,1], this fallback is not meaningful: we set var=0 and still return.
            def _safe_var(m: float) -> float:
                if 0.0 <= m <= 1.0:
                    return m * (1.0 - m)
                return 0.0

            var_c = _safe_var(mean_c)
            var_t = _safe_var(mean_t)

        res = _welch_from_stats(metric, n_c, mean_c, float(var_c), n_t, mean_t, float(var_t))
        out.append(res.to_dict())

    return out
