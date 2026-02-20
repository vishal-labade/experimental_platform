from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Dict

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)


def _load_yaml(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    with p.open("r") as f:
        return yaml.safe_load(f) or {}


def _merge_dicts(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base or {})
    for k, v in (override or {}).items():
        out[k] = v
    return out


def resolve_synth_params(config_path: str, experiment_id: str) -> Dict[str, Any]:
    """
    Matches your configs/dev.yaml layout:

    synthetic:
      default: {...}
      scenarios:
        exp_pricing_test_003: {...}

    Also supports older alternative layouts if you ever change it.
    """
    cfg = _load_yaml(config_path)

    # ---- PRIMARY PATH: cfg["synthetic"]["default"] + cfg["synthetic"]["scenarios"][experiment_id] ----
    syn = cfg.get("synthetic", {})
    if isinstance(syn, dict):
        defaults = syn.get("default", {}) if isinstance(syn.get("default", {}), dict) else {}
        scenarios = syn.get("scenarios", {}) if isinstance(syn.get("scenarios", {}), dict) else {}
        overrides = scenarios.get(experiment_id, {}) if isinstance(scenarios.get(experiment_id, {}), dict) else {}

        resolved = _merge_dicts(defaults, overrides)
        resolved["experiment_id"] = experiment_id
        return resolved

    # ---- FALLBACKS (if YAML changes later) ----
    # A) experiment_id at top-level
    if experiment_id in cfg and isinstance(cfg[experiment_id], dict):
        resolved = dict(cfg[experiment_id])
        resolved["experiment_id"] = experiment_id
        return resolved

    # B) default + nested overrides at top-level
    defaults = cfg.get("default", {}) if isinstance(cfg.get("default", {}), dict) else {}
    exp_overrides: Dict[str, Any] = {}
    for key in ["experiments", "experiment_overrides", "scenarios"]:
        if key in cfg and isinstance(cfg[key], dict):
            exp_overrides = cfg[key].get(experiment_id, {}) or {}
            break

    resolved = _merge_dicts(defaults, exp_overrides)
    resolved["experiment_id"] = experiment_id
    return resolved


_PARAMS_SCHEMA = StructType([
    StructField("experiment_id", StringType(), False),

    StructField("seed", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("days", IntegerType(), True),
    StructField("n_users", IntegerType(), True),

    StructField("exposure_rate", DoubleType(), True),
    StructField("treatment_share", DoubleType(), True),
    StructField("base_conversion", DoubleType(), True),
    StructField("effect_lift_conversion", DoubleType(), True),

    StructField("revenue_mean", DoubleType(), True),
    StructField("revenue_sd", DoubleType(), True),
    StructField("pre_period_days", IntegerType(), True),

    StructField("created_ts", TimestampType(), False),
])


def ingest_experiment_params(
    spark: SparkSession,
    config_path: str,
    catalog: str,
    namespace: str,
    experiment_id: str,
) -> None:
    tbl = f"{catalog}.{namespace}.experiment_params"
    p = resolve_synth_params(config_path=config_path, experiment_id=experiment_id)

    # Guardrail: ensure we actually got params
    non_null_keys = [k for k, v in p.items() if k != "experiment_id" and v is not None]
    if len(non_null_keys) == 0:
        cfg = _load_yaml(config_path)
        syn = cfg.get("synthetic", {})
        scen_keys = []
        if isinstance(syn, dict) and isinstance(syn.get("scenarios", {}), dict):
            scen_keys = list(syn["scenarios"].keys())
        raise ValueError(
            f"Could not resolve synth params for experiment_id={experiment_id} from {config_path}. "
            f"Available synthetic.scenarios keys: {scen_keys[:50]}"
        )

    def _get_int(k: str):
        v = p.get(k)
        return int(v) if v is not None else None

    def _get_float(k: str):
        v = p.get(k)
        return float(v) if v is not None else None

    row = {
        "experiment_id": str(p.get("experiment_id", experiment_id)),
        "seed": _get_int("seed"),
        "start_date": str(p["start_date"]) if p.get("start_date") is not None else None,
        "days": _get_int("days"),
        "n_users": _get_int("n_users"),
        "exposure_rate": _get_float("exposure_rate"),
        "treatment_share": _get_float("treatment_share"),
        "base_conversion": _get_float("base_conversion"),
        "effect_lift_conversion": _get_float("effect_lift_conversion"),
        "revenue_mean": _get_float("revenue_mean"),
        "revenue_sd": _get_float("revenue_sd"),
        "pre_period_days": _get_int("pre_period_days"),
        "created_ts": dt.datetime.utcnow(),
    }

    df = spark.createDataFrame([row], schema=_PARAMS_SCHEMA)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {tbl} (
          experiment_id STRING,
          seed INT,
          start_date STRING,
          days INT,
          n_users INT,
          exposure_rate DOUBLE,
          treatment_share DOUBLE,
          base_conversion DOUBLE,
          effect_lift_conversion DOUBLE,
          revenue_mean DOUBLE,
          revenue_sd DOUBLE,
          pre_period_days INT,
          created_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (experiment_id)
        """
    )

    df.writeTo(tbl).overwrite(col("experiment_id") == experiment_id)
    print(f"[ingest_experiment_params] wrote 1 row to {tbl} experiment_id={experiment_id}")
