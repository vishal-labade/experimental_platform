from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import yaml


@dataclass
class Config:
    data: dict

    @staticmethod
    def load(path: str) -> "Config":
        p = Path(path)

        # If relative, interpret relative to repo root (one level above exp_platform/)
        if not p.is_absolute():
            repo_root = Path(__file__).resolve().parents[1]  # /workspace
            p = repo_root / p

        data = yaml.safe_load(p.read_text())
        return Config(data)

    def get(self, key: str, default=None):
        cur = self.data
        for part in key.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                return default
        return cur
