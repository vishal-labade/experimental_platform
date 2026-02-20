from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional, List

from pydantic import BaseModel, Field


class Exposure(BaseModel):
    experiment_id: str
    user_id: str
    variant: Literal["control", "treatment"]
    exposure_ts: datetime
    unit: Literal["user_id"] = "user_id"


class Outcome(BaseModel):
    experiment_id: str
    user_id: str
    metric_name: str
    value: float
    outcome_ts: datetime


class ExperimentRegistry(BaseModel):
    experiment_id: str
    name: str
    start_ts: datetime
    end_ts: datetime
    owner: Optional[str] = None
    variants: List[str] = Field(min_length=2)
    primary_metrics: List[str] = Field(min_length=1)
    guardrail_metrics: List[str] = Field(default_factory=list)
    notes: Optional[str] = None
