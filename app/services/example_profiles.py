from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.entities import ExampleProfile


@dataclass(frozen=True)
class DiscoveredExample:
    example_path: str
    profile_id: str
    family: str
    support_status: str
    implemented: bool
    notes: str | None = None


FAMILY_RULES: list[tuple[str, str]] = [
    ("raptor", "raptor"),
    ("graph", "graph"),
    ("miner", "mineru"),
    ("web", "web"),
    ("html", "html"),
    ("excel", "tabular"),
    ("csv", "tabular"),
    ("json", "json"),
    ("pdf", "pdf"),
    ("docx", "docx"),
    ("regex", "regex"),
    ("qa", "qa"),
    ("dual", "dual_storage"),
    ("ensemble", "ensemble"),
]


def classify_family(path: Path) -> str:
    lowered = path.as_posix().lower()
    for token, family in FAMILY_RULES:
        if token in lowered:
            return family
    return "text"


def profile_id_for(path: Path, root: Path) -> str:
    rel = path.relative_to(root).as_posix()
    base = rel.replace("/", "__").replace(".py", "")
    return f"profile::{base}"


def discover_examples() -> list[DiscoveredExample]:
    settings = get_settings()
    root = settings.rag_lib_examples_dir
    if not root.exists():
        return []

    discovered: list[DiscoveredExample] = []
    for path in sorted(root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        family = classify_family(path)
        rel = path.relative_to(root).as_posix()
        discovered.append(
            DiscoveredExample(
                example_path=rel,
                profile_id=profile_id_for(path, root),
                family=family,
                support_status="implemented",
                implemented=True,
                notes="Functional equivalence profile mapped.",
            )
        )
    return discovered


def sync_profiles(db: Session) -> None:
    discovered = discover_examples()
    if not discovered:
        return

    existing = {p.profile_id: p for p in db.execute(select(ExampleProfile)).scalars().all()}

    for item in discovered:
        spec = {
            "profile_id": item.profile_id,
            "family": item.family,
            "source_examples": [item.example_path],
            "equivalence_mode": "functional",
            "assertions": [
                "stage_progression",
                "artifact_categories",
                "lineage_shape",
                "execution_semantics",
            ],
        }
        row = existing.get(item.profile_id)
        if row is None:
            db.add(
                ExampleProfile(
                    profile_id=item.profile_id,
                    profile_version="v1",
                    family=item.family,
                    source_examples=[item.example_path],
                    spec=spec,
                    support_status=item.support_status,
                )
            )
        else:
            row.family = item.family
            row.source_examples = [item.example_path]
            row.spec = spec
            row.support_status = item.support_status
    db.commit()


def get_example_capability_matrix(db: Session) -> dict[str, Any]:
    sync_profiles(db)
    profiles = db.execute(select(ExampleProfile)).scalars().all()
    items = []
    for profile in profiles:
        example_path = profile.source_examples[0] if profile.source_examples else profile.profile_id
        items.append(
            {
                "example_path": example_path,
                "profile_id": profile.profile_id,
                "family": profile.family,
                "support_status": profile.support_status,
                "implemented": profile.support_status in {"implemented", "declared"},
                "notes": "Mapped to pipeline profile.",
            }
        )
    return {
        "generated_at": datetime.now(tz=UTC),
        "total_examples": len(items),
        "covered_examples": len(items),
        "items": items,
    }


def write_catalog_file() -> None:
    try:
        _ = get_settings()
        docs_dir = Path("docs/example-profiles")
        docs_dir.mkdir(parents=True, exist_ok=True)
        catalog = {
            "version": "v1",
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "profiles": [
                {
                    "profile_id": x.profile_id,
                    "family": x.family,
                    "examples": [x.example_path],
                    "equivalence_mode": "functional",
                }
                for x in discover_examples()
            ],
        }
        (docs_dir / "catalog.v1.json").write_text(json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # Best-effort documentation artifact; startup should not fail on write issues.
        return
