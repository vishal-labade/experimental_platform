from __future__ import annotations

from exp_platform.config import Config
from exp_platform.synth.generate import SynthParams, generate_synthetic


def main():
    cfg = Config.load("configs/dev.yaml")

    p = SynthParams(
        seed=int(cfg.get("synthetic.seed")),
        experiment_id=str(cfg.get("synthetic.experiment_id")),
        start_date=str(cfg.get("synthetic.start_date")),
        days=int(cfg.get("synthetic.days")),
        n_users=int(cfg.get("synthetic.n_users")),
        exposure_rate=float(cfg.get("synthetic.exposure_rate")),
        treatment_share=float(cfg.get("synthetic.treatment_share")),
        base_conversion=float(cfg.get("synthetic.base_conversion")),
        effect_lift_conversion=float(cfg.get("synthetic.effect_lift_conversion")),
        revenue_mean=float(cfg.get("synthetic.revenue_mean")),
        revenue_sd=float(cfg.get("synthetic.revenue_sd")),
        pre_period_days=int(cfg.get("synthetic.pre_period_days")),
    )

    exposures, outcomes, registry = generate_synthetic(p)

    # Write to MinIO using s3fs via pandas
    # IMPORTANT: use HOST-mapped MinIO endpoint, since you’re on host network
    endpoint = "http://10.0.0.80:9100"
    access_key = "minioadmin"
    secret_key = "minioadmin"

    # Raw bucket/prefix
    base = f"s3://raw/exp_platform/{p.experiment_id}"

    storage_options = {
        "key": access_key,
        "secret": secret_key,
        "client_kwargs": {"endpoint_url": endpoint},
    }

    exposures.to_parquet(f"{base}/exposures.parquet", index=False, storage_options=storage_options)
    outcomes.to_parquet(f"{base}/outcomes.parquet", index=False, storage_options=storage_options)
    registry.to_parquet(f"{base}/experiment_registry.parquet", index=False, storage_options=storage_options)

    print("Wrote to MinIO:")
    print(f" - {base}/exposures.parquet rows={len(exposures):,}")
    print(f" - {base}/outcomes.parquet rows={len(outcomes):,}")
    print(f" - {base}/experiment_registry.parquet rows={len(registry):,}")


if __name__ == "__main__":
    main()
