from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from exp_platform.config import Config
from exp_platform.io.paths import Paths
from exp_platform.synth.generate import SynthParams, generate_synthetic


def write_parquet(df, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, path)


def main():
    cfg = Config.load("configs/dev.yaml")
    paths = Paths.from_config(cfg)
    paths.ensure()

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

    out_base = Path(cfg.get("storage.raw_dir", "data/raw")) / p.experiment_id
    write_parquet(exposures, out_base / "exposures.parquet")
    write_parquet(outcomes, out_base / "outcomes.parquet")
    write_parquet(registry, out_base / "experiment_registry.parquet")

    print("Wrote:")
    print(" -", out_base / "exposures.parquet", f"rows={len(exposures):,}")
    print(" -", out_base / "outcomes.parquet", f"rows={len(outcomes):,}")
    print(" -", out_base / "experiment_registry.parquet", f"rows={len(registry):,}")


if __name__ == "__main__":
    main()
