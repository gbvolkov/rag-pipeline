from __future__ import annotations

import json
from pathlib import Path

if __package__ in {None, ""}:
    raise SystemExit("Run this script as a module: python -m scripts.run_example_conformance")

from scripts.lib.example_profiles import get_example_capability_matrix, write_catalog_file


def main() -> int:
    write_catalog_file()
    report = get_example_capability_matrix()

    reports_dir = Path("reports")
    reports_dir.mkdir(parents=True, exist_ok=True)
    output_path = reports_dir / "example_conformance_report.json"
    output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    unsupported = [item for item in report.get("items", []) if item.get("support_status") == "unsupported"]
    summary = {
        "report": output_path.as_posix(),
        "total_examples": report.get("total_examples"),
        "covered_examples": report.get("covered_examples"),
        "unsupported_examples": len(unsupported),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 1 if unsupported else 0


if __name__ == "__main__":
    raise SystemExit(main())
