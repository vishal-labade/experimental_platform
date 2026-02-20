from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_MEMO_DIR = "data/memos"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def memo_path(experiment_id: str, memo_dir: str = DEFAULT_MEMO_DIR) -> Path:
    p = Path(memo_dir)
    p.mkdir(parents=True, exist_ok=True)
    return p / f"{experiment_id}.md"


def init_memo(
    experiment_id: str,
    confidence: float = 0.95,
    overwrite: bool = False,
    memo_dir: str = DEFAULT_MEMO_DIR,
) -> Path:
    """
    Create the memo file if missing. If overwrite=True, regenerate header from scratch.
    Returns the memo Path.
    """
    path = memo_path(experiment_id, memo_dir=memo_dir)

    if path.exists() and not overwrite:
        return path

    header = f"""# Experiment Decision Memo (Draft)

**Experiment:** `{experiment_id}`

**Confidence:** {confidence}

**Data source:** `iceberg.exp.metric_aggregates_overall` → `iceberg.exp.analysis_results`

> Generated at: `{_utcnow_iso()}`

---

"""
    path.write_text(header, encoding="utf-8")
    return path


def _markers(stage: str) -> tuple[str, str]:
    s = stage.strip().upper()
    return (f"<!-- BEGIN:{s} -->", f"<!-- END:{s} -->")


def upsert_stage(memo_path: Path, stage: str, md: str) -> None:
    """
    Idempotently replace a stage block if present; otherwise append.
    """
    start, end = _markers(stage)
    block = f"{start}\n{md.rstrip()}\n{end}\n"

    if memo_path.exists():
        content = memo_path.read_text(encoding="utf-8")
    else:
        content = ""

    if start in content and end in content:
        pre = content.split(start, 1)[0]
        post = content.split(end, 1)[1]
        new_content = pre + block + post
    else:
        if content and not content.endswith("\n"):
            content += "\n"
        new_content = content + "\n" + block

    tmp = memo_path.with_suffix(memo_path.suffix + ".tmp")
    tmp.write_text(new_content, encoding="utf-8")
    tmp.replace(memo_path)
