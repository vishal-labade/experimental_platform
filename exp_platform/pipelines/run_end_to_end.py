from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from typing import List
from exp_platform.memo import init_memo



@dataclass
class Step:
    name: str
    cmd: List[str]
    optional: bool = False


def _run(step: Step, dry_run: bool = False) -> None:
    print(f"\n=== [{step.name}] ===")
    print(" ".join(step.cmd))

    if dry_run:
        print("(dry-run) skipped execution")
        return

    subprocess.run(step.cmd, check=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run experimentation platform end-to-end for ONE experiment_id.")

    # Core
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--python", default=sys.executable, help="Python executable to use")
    p.add_argument("--dry-run", action="store_true")

    # Config passthrough (used by generate_synth + run_ingest)
    p.add_argument("--config", default="configs/dev.yaml", help="Path to config YAML")

    # Optional: generate synthetic raw parquet before ingest (Option 1 scenarios live in YAML)
    p.add_argument("--generate-synth", action="store_true")
    p.add_argument("--synth-target", choices=["local", "minio"], default="minio")

    # Which analyses to run
    p.add_argument("--skip-dq", action="store_true")
    p.add_argument("--skip-metrics", action="store_true")
    p.add_argument("--skip-ab", action="store_true")
    p.add_argument("--skip-cuped", action="store_true")
    p.add_argument("--skip-did", action="store_true")
    p.add_argument("--skip-pretrends", action="store_true")

    # Pretrends args
    p.add_argument("--pretrends-metric", default="pre_revenue")
    p.add_argument("--pretrends-grain", choices=["day", "week"], default="day")
    p.add_argument("--pre-buckets", type=int, default=28)
    p.add_argument("--post-buckets", type=int, default=28)

    return p.parse_args()


def main() -> None:
    args = parse_args()
    py = args.python

    steps: List[Step] = []

    init_memo(
    experiment_id=args.experiment_id,
    confidence=0.95,
    overwrite=True,   # clean memo on rerun
)


    # 0) Generate synth (ad-hoc per experiment_id; Option 1 YAML scenarios)
    if args.generate_synth:
        steps.append(
            Step(
                name="generate_synth",
                cmd=[
                    py, "-m", "exp_platform.generate_synth",
                    "--config", args.config,
                    "--target", args.synth_target,
                    "--experiment-id", args.experiment_id,
                ],
            )
        )

    # 1) Ingest raw parquet -> Iceberg (MUST receive same config as synth)
    steps.append(
        Step(
            name="ingest_all",
            cmd=[
                py, "-m", "exp_platform.pipelines.run_ingest",
                "--config", args.config,
                "--experiment-id", args.experiment_id,
            ],
        )
    )

    # 2) Data quality report
    if not args.skip_dq:
        steps.append(
            Step(
                name="dq_report",
                cmd=[py, "-m", "exp_platform.pipelines.dq_report", "--experiment-id", args.experiment_id],
            )
        )

    # 3) Metric aggregates
    if not args.skip_metrics:
        steps.append(
            Step(
                name="compute_metric_aggregates",
                cmd=[py, "-m", "exp_platform.pipelines.compute_metric_aggregates", "--experiment-id", args.experiment_id],
            )
        )

    # 4) A/B analysis
    if not args.skip_ab:
        steps.append(
            Step(
                name="analyze_experiment_ab",
                cmd=[py, "-m", "exp_platform.pipelines.analyze_experiment", "--experiment-id", args.experiment_id],
            )
        )

    # 5) CUPED analysis
    if not args.skip_cuped:
        steps.append(
            Step(
                name="analyze_experiment_cuped",
                cmd=[py, "-m", "exp_platform.pipelines.analyze_experiment_cuped", "--experiment-id", args.experiment_id],
            )
        )

    # 6) DiD analysis
    if not args.skip_did:
        steps.append(
            Step(
                name="analyze_experiment_did",
                cmd=[py, "-m", "exp_platform.pipelines.analyze_experiment_did", "--experiment-id", args.experiment_id],
            )
        )

    # 7) Pretrends (optional best-effort, no plots)
    if not args.skip_pretrends:
        steps.append(
            Step(
  name="did_pretrends",
  cmd=[
    py, "-m", "exp_platform.pipelines.did_pretrends",
    "--experiment-id", args.experiment_id,
    "--metric-pre", "pre_revenue",
  ],
  optional=True,
)
        )



    # Execute
    for step in steps:
        try:
            _run(step, dry_run=args.dry_run)
        except subprocess.CalledProcessError as e:
            if step.optional:
                print(f"⚠️ Optional step failed, continuing: {step.name} (exit={e.returncode})")
                continue
            print(f"\n❌ Step failed: {step.name} (exit={e.returncode})")
            raise

    print("\n✅ End-to-end run complete.")
    print(f"- experiment_id: {args.experiment_id}")
    print(f"- memo: data/memos/{args.experiment_id}.md")
    print("- iceberg outputs: metric_aggregates_*, analysis_results, analysis_results_cuped, did_results, did_pretrend_*")


if __name__ == "__main__":
    main()
