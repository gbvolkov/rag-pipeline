# Example Profiles

`catalog.v1.json` is the canonical mapping from `C:/Projects/rag-lib/examples` to API profile IDs.

Each profile is validated at functional-equivalence level:

- same stage progression semantics,
- same artifact category outputs (documents/segments/indices/results),
- same lineage directionality and version tracking,
- same failure propagation semantics for missing optional dependencies.

Use:

- `uv run python scripts/check_parity_drift.py` to fail on catalog drift.
- `uv run python scripts/run_example_conformance.py` to execute mapped profiles through API endpoints.

