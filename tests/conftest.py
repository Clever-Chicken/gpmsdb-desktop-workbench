from pathlib import Path

import pytest


def _has_real_test_files() -> bool:
  tests_dir = Path(__file__).resolve().parent
  patterns = ("test_*.py", "*_test.py")

  for pattern in patterns:
    for path in tests_dir.rglob(pattern):
      if path.name != "conftest.py":
        return True

  return False


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
  if (
    exitstatus == pytest.ExitCode.NO_TESTS_COLLECTED
    and not _has_real_test_files()
  ):
    session.exitstatus = pytest.ExitCode.OK
