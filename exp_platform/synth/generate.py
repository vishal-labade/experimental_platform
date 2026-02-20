from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Tuple

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SynthParams:
    seed: int
    experiment_id: str
    start_date: str           # YYYY-MM-DD
    days: int
    n_users: int
    exposure_rate: float
    treatment_share: float
    base_conversion: float
    effect_lift_conversion: float  # absolute lift
    revenue_mean: float
    revenue_sd: float
    pre_period_days: int


def _utc_dt(date_str: str) -> datetime:
    # midnight UTC for deterministic partitioning; you can later move to your TZ
    return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)


def generate_synthetic(params: SynthParams) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(params.seed)

    start = _utc_dt(params.start_date)
    end = start + timedelta(days=params.days)

    # users
    user_ids = np.array([f"u_{i:07d}" for i in range(params.n_users)])

    # who is exposed?
    exposed = rng.random(params.n_users) < params.exposure_rate
    exposed_users = user_ids[exposed]

    # random assignment (simple)
    treatment = rng.random(exposed_users.shape[0]) < params.treatment_share
    variants = np.where(treatment, "treatment", "control")

    # exposure timestamps uniform over window
    exposure_seconds = rng.integers(0, int((end - start).total_seconds()), size=exposed_users.shape[0])
    exposure_ts = [start + timedelta(seconds=int(s)) for s in exposure_seconds]

    exposures = pd.DataFrame(
        {
            "experiment_id": params.experiment_id,
            "user_id": exposed_users,
            "variant": variants,
            "exposure_ts": pd.to_datetime(exposure_ts, utc=True),
            "unit": "user_id",
        }
    )

    # pre-period covariate: baseline revenue/user (for CUPED later)
    pre_start = start - timedelta(days=params.pre_period_days)
    pre_seconds = rng.integers(0, int((start - pre_start).total_seconds()), size=params.n_users)
    pre_ts = [pre_start + timedelta(seconds=int(s)) for s in pre_seconds]
    pre_revenue = np.maximum(0.0, rng.normal(params.revenue_mean, params.revenue_sd, size=params.n_users))

    outcomes_pre = pd.DataFrame(
        {
            "experiment_id": params.experiment_id,
            "user_id": user_ids,
            "metric_name": "pre_revenue",
            "value": pre_revenue.astype(float),
            "outcome_ts": pd.to_datetime(pre_ts, utc=True),
        }
    )

    # post outcomes only for exposed users (common pattern)
    # conversion probability
    p = np.where(variants == "treatment",
                 params.base_conversion + params.effect_lift_conversion,
                 params.base_conversion)
    conv = (rng.random(exposed_users.shape[0]) < p).astype(int)

    # revenue conditional on conversion
    post_rev = np.where(conv == 1,
                        np.maximum(0.0, rng.normal(params.revenue_mean, params.revenue_sd, size=conv.shape[0])),
                        0.0)

    # outcome timestamps after exposure (within same day)
    lag_seconds = rng.integers(60, 6 * 3600, size=exposed_users.shape[0])
    outcome_ts = [ts + timedelta(seconds=int(l)) for ts, l in zip(exposure_ts, lag_seconds)]

    outcomes_post = pd.DataFrame(
        {
            "experiment_id": params.experiment_id,
            "user_id": exposed_users,
            "metric_name": "conversion",
            "value": conv.astype(float),
            "outcome_ts": pd.to_datetime(outcome_ts, utc=True),
        }
    )

    outcomes_post_rev = pd.DataFrame(
        {
            "experiment_id": params.experiment_id,
            "user_id": exposed_users,
            "metric_name": "revenue",
            "value": post_rev.astype(float),
            "outcome_ts": pd.to_datetime(outcome_ts, utc=True),
        }
    )

    outcomes = pd.concat([outcomes_pre, outcomes_post, outcomes_post_rev], ignore_index=True)

    registry = pd.DataFrame(
        [{
            "experiment_id": params.experiment_id,
            "name": "Checkout button color test",
            "start_ts": pd.to_datetime(start, utc=True),
            "end_ts": pd.to_datetime(end, utc=True),
            "owner": "you",
            "variants": ["control", "treatment"],
            "primary_metrics": ["conversion"],
            "guardrail_metrics": ["revenue"],
            "notes": "Synthetic demo experiment"
        }]
    )

    return exposures, outcomes, registry
