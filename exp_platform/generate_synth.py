from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional

from exp_platform.config import Config
from exp_platform.synth.generate import SynthParams, generate_synthetic


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate synthetic experiment data")
    p.add_argument("--config", default="configs/dev.yaml", help="Path to config YAML")
    p.add_argument("--target", choices=["local", "minio"], default="minio", help="Write target")
    p.add_argument(
        "--experiment-id",
        default=None,
        help="Experiment to generate (must exist in synthetic.scenarios, else uses defaults).",
    )
    return p.parse_args()


def _as_dict(x: Any) -> Dict[str, Any]:
    return x if isinstance(x, dict) else {}


def _resolve_synth_config(cfg: Config, experiment_id: str) -> Dict[str, Any]:
    """
    Supports both:
      NEW:
        synthetic.default
        synthetic.scenarios.<experiment_id>
      OLD (backward-compatible):
        synthetic.seed, synthetic.n_users, ...
        synthetic.experiment_id
    """
    # New structure
    defaults = _as_dict(cfg.get("synthetic.default", None))
    scenarios = _as_dict(cfg.get("synthetic.scenarios", None))
    overrides = _as_dict(scenarios.get(experiment_id, {})) if scenarios else {}

    if defaults or scenarios:
        merged = dict(defaults)
        merged.update(overrides)
        merged["experiment_id"] = experiment_id
        return merged

    # Old structure fallback
    merged = {
        "seed": cfg.get("synthetic.seed"),
        "experiment_id": experiment_id,
        "start_date": cfg.get("synthetic.start_date"),
        "days": cfg.get("synthetic.days"),
        "n_users": cfg.get("synthetic.n_users"),
        "exposure_rate": cfg.get("synthetic.exposure_rate"),
        "treatment_share": cfg.get("synthetic.treatment_share"),
        "base_conversion": cfg.get("synthetic.base_conversion"),
        "effect_lift_conversion": cfg.get("synthetic.effect_lift_conversion"),
        "revenue_mean": cfg.get("synthetic.revenue_mean"),
        "revenue_sd": cfg.get("synthetic.revenue_sd"),
        "pre_period_days": cfg.get("synthetic.pre_period_days"),
    }
    return merged


def _require(d: Dict[str, Any], k: str) -> Any:
    if k not in d or d[k] is None:
        raise ValueError(f"Missing required synthetic param: {k}")
    return d[k]


def main() -> None:
    args = _parse_args()
    cfg = Config.load(args.config)

    # Determine experiment_id
    exp_id = args.experiment_id
    if not exp_id:
        # Try new-style default experiment_id, else old
        exp_id = cfg.get("synthetic.default.experiment_id", None) or cfg.get("synthetic.experiment_id", None)
    if not exp_id:
        raise ValueError(
            "No experiment_id provided. Pass --experiment-id or set synthetic.scenarios.<id> and run with --experiment-id."
        )

    sc = _resolve_synth_config(cfg, exp_id)

    params = SynthParams(
        seed=int(_require(sc, "seed")),
        experiment_id=str(exp_id),
        start_date=str(_require(sc, "start_date")),
        days=int(_require(sc, "days")),
        n_users=int(_require(sc, "n_users")),
        exposure_rate=float(_require(sc, "exposure_rate")),
        treatment_share=float(_require(sc, "treatment_share")),
        base_conversion=float(_require(sc, "base_conversion")),
        effect_lift_conversion=float(_require(sc, "effect_lift_conversion")),
        revenue_mean=float(_require(sc, "revenue_mean")),
        revenue_sd=float(_require(sc, "revenue_sd")),
        pre_period_days=int(_require(sc, "pre_period_days")),
    )

    exposures, outcomes, registry = generate_synthetic(params)

    if args.target == "local":
        raw_root = cfg.get("paths.local_raw_root", None) or cfg.get("paths.local_out_dir", "data/raw")
        out_dir = Path(str(raw_root)) / params.experiment_id
        out_dir.mkdir(parents=True, exist_ok=True)

        exposures.to_parquet(out_dir / "exposures.parquet", index=False)
        outcomes.to_parquet(out_dir / "outcomes.parquet", index=False)
        registry.to_parquet(out_dir / "experiment_registry.parquet", index=False)

        print("Wrote LOCAL parquet:")
        print(f" - {out_dir}/exposures.parquet rows={len(exposures):,}")
        print(f" - {out_dir}/outcomes.parquet rows={len(outcomes):,}")
        print(f" - {out_dir}/experiment_registry.parquet rows={len(registry):,}")
        print("\nResolved synthetic params:")
        print(asdict(params))
        return

    # target == "minio"
    endpoint = str(cfg.get("minio.endpoint"))
    access_key = str(cfg.get("minio.access_key"))
    secret_key = str(cfg.get("minio.secret_key"))
    raw_bucket = str(cfg.get("minio.raw_bucket", "raw"))

    base = f"s3://{raw_bucket}/exp_platform/{params.experiment_id}"
    storage_options = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": endpoint},
    }

    exposures.to_parquet(f"{base}/exposures.parquet", index=False, storage_options=storage_options)
    outcomes.to_parquet(f"{base}/outcomes.parquet", index=False, storage_options=storage_options)
    registry.to_parquet(f"{base}/experiment_registry.parquet", index=False, storage_options=storage_options)

    print("Wrote MinIO parquet:")
    print(f" - {base}/exposures.parquet rows={len(exposures):,}")
    print(f" - {base}/outcomes.parquet rows={len(outcomes):,}")
    print(f" - {base}/experiment_registry.parquet rows={len(registry):,}")
    print("\nResolved synthetic params:")
    print(asdict(params))


if __name__ == "__main__":
    main()
