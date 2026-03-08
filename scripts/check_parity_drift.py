from __future__ import annotations

import json
from pathlib import Path

if __package__ in {None, ""}:
    raise SystemExit("Run this script as a module: python -m scripts.check_parity_drift")

from app.services.capabilities import discover_capabilities
from scripts.lib.example_profiles import discover_examples


def main() -> int:
    capabilities = discover_capabilities()
    examples = discover_examples()

    catalog_path = Path("docs/example-profiles/catalog.v1.json")
    if not catalog_path.exists():
        print("ERROR: missing docs/example-profiles/catalog.v1.json")
        return 1
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    catalog_profiles = {profile["profile_id"] for profile in catalog.get("profiles", [])}

    discovered_profiles = {item.profile_id for item in examples}
    missing = sorted(discovered_profiles - catalog_profiles)
    extra = sorted(catalog_profiles - discovered_profiles)

    strict = capabilities.get("strict", {})
    loaders = strict.get("loaders", {})
    splitters = strict.get("splitters", {})
    retrievers = strict.get("retrievers", {}).get("classes", {})

    print(f"loaders={len(loaders)} splitters={len(splitters)} retrievers={len(retrievers)} examples={len(examples)}")
    if missing:
        print("ERROR: missing profile mappings in catalog:")
        for item in missing:
            print(f"  - {item}")
        return 1
    if extra:
        print("ERROR: stale profile mappings in catalog:")
        for item in extra:
            print(f"  - {item}")
        return 1
    print("Parity drift check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
