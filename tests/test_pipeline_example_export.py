from __future__ import annotations

import json
import re
from pathlib import Path

from scripts.lib.pipeline_example_export import SnapshotExporter, make_run_root


def test_make_run_root_creates_timestamped_directory(tmp_path: Path) -> None:
    run_root = make_run_root(tmp_path)
    assert run_root.exists()
    assert run_root.is_dir()
    assert run_root.parent == tmp_path
    assert re.match(r"^\d{8}T\d{6}Z$", run_root.name)


def test_snapshot_exporter_writes_example_json_with_pretty_format(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    run_root.mkdir(parents=True, exist_ok=True)
    exporter = SnapshotExporter(run_root=run_root)

    written_path = exporter.write_example_json(
        "example/unsafe:id",
        "payload.json",
        {"zeta": 2, "alpha": {"beta": 1}},
    )
    assert written_path.exists()
    assert written_path.parent.name == "example_unsafe_id"

    raw = written_path.read_text(encoding="utf-8")
    assert raw.startswith("{\n")
    assert '\n  "alpha": {\n' in raw
    assert raw.find('"alpha"') < raw.find('"zeta"')
    parsed = json.loads(raw)
    assert parsed["alpha"]["beta"] == 1


def test_snapshot_exporter_writes_run_level_json(tmp_path: Path) -> None:
    run_root = tmp_path / "run"
    run_root.mkdir(parents=True, exist_ok=True)
    exporter = SnapshotExporter(run_root=run_root)

    path = exporter.write_run_json("run.summary.json", {"total_executed": 2})
    assert path == run_root / "run.summary.json"
    assert json.loads(path.read_text(encoding="utf-8"))["total_executed"] == 2

