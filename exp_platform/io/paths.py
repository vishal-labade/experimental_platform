from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    base_dir: Path
    raw_dir: Path
    curated_dir: Path

    @staticmethod
    def from_config(cfg) -> "Paths":
        base = Path(cfg.get("storage.base_dir", "data"))
        raw = Path(cfg.get("storage.raw_dir", str(base / "raw")))
        curated = Path(cfg.get("storage.curated_dir", str(base / "curated")))
        return Paths(base_dir=base, raw_dir=raw, curated_dir=curated)

    def ensure(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.curated_dir.mkdir(parents=True, exist_ok=True)
