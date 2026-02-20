from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class CupedTheta:
    metric_name: str
    covariate_name: str
    theta: float
    x_mean: float
    n: int


def safe_div(a: float, b: float) -> Optional[float]:
    if b == 0.0:
        return None
    return a / b


def compute_theta_from_moments(
    metric_name: str,
    covariate_name: str,
    n: int,
    x_mean: float,
    y_mean: float,
    x2_mean: float,
    y2_mean: float,
    xy_mean: float,
) -> CupedTheta:
    """
    theta = Cov(X,Y) / Var(X)
    where:
      Var(X) = E[X^2] - (E[X])^2
      Cov(X,Y)= E[XY] - E[X]E[Y]
    """
    var_x = x2_mean - (x_mean ** 2)
    cov_xy = xy_mean - (x_mean * y_mean)

    if var_x <= 0.0:
        theta = 0.0
    else:
        theta = cov_xy / var_x

    return CupedTheta(
        metric_name=metric_name,
        covariate_name=covariate_name,
        theta=float(theta),
        x_mean=float(x_mean),
        n=int(n),
    )


def apply_cuped(y: float, x: float, theta: float, x_mean: float) -> float:
    # y_adj = y - theta*(x - x_mean)
    return y - theta * (x - x_mean)
