from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from tests.fixtures.small_source.build_fixture import build_fixture


def test_cli_converts_fixture_directory(tmp_path: Path) -> None:
    src = tmp_path / "source"
    build_fixture(src)
    out = tmp_path / "out"

    result = subprocess.run(
        [
            sys.executable,
            "tools/pickle_to_mmap/convert_gpmsdb.py",
            "--source-dir",
            str(src),
            "--out-dir",
            str(out),
        ],
        capture_output=True,
        text=True,
        check=False,
        cwd=Path(__file__).resolve().parents[2],
    )

    assert result.returncode == 0
    assert (out / "header.bin").exists()
    assert (out / "mass_index.bin").exists()
    assert (out / "genome_peaks.bin").exists()
    assert (out / "meta.bin").exists()
