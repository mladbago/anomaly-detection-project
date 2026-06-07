# tests/_runner.py
"""Tiny zero-dependency test helpers.

The suite is written to run **either** under ``pytest`` **or** with a plain
``python tests/run_all.py`` (handy inside the slim containers, which don't ship
pytest). ``skip()`` defers to pytest's skip when it's importable, otherwise it
raises :class:`Skipped`, which the bundled runner reports as SKIP rather than
failure -- so infra-dependent integration tests degrade gracefully.
"""


class Skipped(Exception):
    """Raised to mark a test as skipped when pytest is unavailable."""


def skip(reason: str):
    """Skip the current test (pytest-aware)."""
    try:
        import pytest
        pytest.skip(reason)
    except ImportError:
        raise Skipped(reason)
