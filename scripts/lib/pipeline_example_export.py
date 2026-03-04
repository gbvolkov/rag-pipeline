from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def make_run_root(results_dir: Path) -> Path:
    timestamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    run_root = results_dir / timestamp
    run_root.mkdir(parents=True, exist_ok=True)
    return run_root


def _sanitize_part(part: str) -> str:
    keep = []
    for char in part:
        if char.isalnum() or char in {"-", "_", "."}:
            keep.append(char)
        else:
            keep.append("_")
    return "".join(keep)


class SnapshotExporter:
    def __init__(self, run_root: Path):
        self.run_root = run_root

    def example_dir(self, example_id: str) -> Path:
        path = self.run_root / _sanitize_part(example_id)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write_example_json(self, example_id: str, filename: str, payload: Any) -> Path:
        path = self.example_dir(example_id) / filename
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return path

    def write_run_json(self, filename: str, payload: Any) -> Path:
        path = self.run_root / filename
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return path

