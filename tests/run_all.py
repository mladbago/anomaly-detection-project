# tests/run_all.py
"""Zero-dependency test runner: `python tests/run_all.py`.

Discovers every ``test_*`` function in the suite, runs it, and reports
PASS / FAIL / SKIP. Use this when pytest isn't installed (e.g. inside the slim
service images). With pytest available, just run ``pytest tests/`` instead.
"""
import importlib
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests._runner import Skipped

TEST_MODULES = [
    "tests.test_schema_validation",
    "tests.test_rules",
    "tests.test_state_reset",
    "tests.test_precision_offline",
    "tests.test_pipeline_integration",
]


def main() -> int:
    passed = failed = skipped = 0
    for mod_name in TEST_MODULES:
        module = importlib.import_module(mod_name)
        print(f"\n=== {mod_name} ===")
        for name in sorted(vars(module)):
            fn = getattr(module, name)
            if not (name.startswith("test_") and callable(fn)):
                continue
            try:
                fn()
                print(f"  PASS  {name}")
                passed += 1
            except Skipped as exc:
                print(f"  SKIP  {name}: {exc}")
                skipped += 1
            except Exception:  # noqa: BLE001
                print(f"  FAIL  {name}")
                traceback.print_exc()
                failed += 1
    print(f"\n{passed} passed, {failed} failed, {skipped} skipped")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
