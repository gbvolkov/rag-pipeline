from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.services.capabilities import discover_capabilities
from app.services.example_profiles import discover_examples


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

    loaders = capabilities.get("loaders", {})
    splitters = capabilities.get("splitters", {})
    retrievers = capabilities.get("retrievers", {}).get("classes", {})

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
