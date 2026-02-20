from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Optional


@dataclass(frozen=True)
class DidResult:
    metric_name: str
    # group means
    y_treat_pre: float
    y_treat_post: float
    y_ctrl_pre: float
    y_ctrl_post: float

    did: float
    se: float
    z: Optional[float]
    p_value: Optional[float]
    ci_low: float
    ci_high: float


def _normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _zcrit(confidence: float) -> float:
    if abs(confidence - 0.90) < 1e-9:
        return 1.6448536269514722
    if abs(confidence - 0.95) < 1e-9:
        return 1.959963984540054
    if abs(confidence - 0.99) < 1e-9:
        return 2.5758293035489004
    raise ValueError("Unsupported confidence. Use 0.90, 0.95, or 0.99.")


def did_from_cell_means(
    metric_name: str,
    y_treat_pre: float,
    y_treat_post: float,
    y_ctrl_pre: float,
    y_ctrl_post: float,
    # standard errors of each cell mean
    se_treat_pre: float,
    se_treat_post: float,
    se_ctrl_pre: float,
    se_ctrl_post: float,
    confidence: float = 0.95,
) -> DidResult:
    """
    DiD = (T_post - T_pre) - (C_post - C_pre)

    SE(DiD) assuming independent cell mean estimates:
      Var = se_tp^2 + se_tpost^2 + se_cp^2 + se_cpost^2
      SE = sqrt(Var)

    This is a clean first-cut. Later we can do clustered SEs.
    """
    did = (y_treat_post - y_treat_pre) - (y_ctrl_post - y_ctrl_pre)

    var = (se_treat_pre**2 + se_treat_post**2 + se_ctrl_pre**2 + se_ctrl_post**2)
    se = math.sqrt(var)

    if se == 0.0:
        z = None
        p = None
        ci_low = did
        ci_high = did
    else:
        z = did / se
        p = 2.0 * (1.0 - _normal_cdf(abs(z)))
        zc = _zcrit(confidence)
        ci_low = did - zc * se
        ci_high = did + zc * se

    return DidResult(
        metric_name=metric_name,
        y_treat_pre=float(y_treat_pre),
        y_treat_post=float(y_treat_post),
        y_ctrl_pre=float(y_ctrl_pre),
        y_ctrl_post=float(y_ctrl_post),
        did=float(did),
        se=float(se),
        z=(float(z) if z is not None else None),
        p_value=(float(p) if p is not None else None),
        ci_low=float(ci_low),
        ci_high=float(ci_high),
    )
