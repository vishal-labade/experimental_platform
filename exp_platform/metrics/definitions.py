from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal


MetricWindow = Literal["pre", "post", "any"]
MetricKind = Literal["mean"]


@dataclass(frozen=True)
class MetricSpec:
    name: str
    kind: MetricKind = "mean"
    window: MetricWindow = "any"   # "pre" for pre-period covariates, "post" for post exposure, "any" no filter


def default_metric_specs() -> List[MetricSpec]:
    """
    Chunk 3 starts simple: mean of value per metric.
    You can add ratio metrics, guardrails, etc. in later chunks.
    """
    return [
        MetricSpec(name="conversion", kind="mean", window="post"),
        MetricSpec(name="revenue", kind="mean", window="post"),
        MetricSpec(name="pre_revenue", kind="mean", window="pre"),
    ]


def specs_by_name(specs: List[MetricSpec]) -> Dict[str, MetricSpec]:
    return {s.name: s for s in specs}
