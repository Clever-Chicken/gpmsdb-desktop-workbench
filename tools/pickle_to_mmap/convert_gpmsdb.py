from __future__ import annotations

from pathlib import Path

from tools.pickle_to_mmap.format import HeaderLayout


def convert_fixture(source_dir: Path, out_dir: Path) -> None:
    source_dir = Path(source_dir)
    out_dir = Path(out_dir)

    out_dir.mkdir(parents=True, exist_ok=True)

    # The Task 4 skeleton only guarantees artifact creation.
    (out_dir / "header.bin").write_bytes(HeaderLayout.MAGIC.ljust(HeaderLayout.SIZE, b"\0"))
    (out_dir / "mass_index.bin").write_bytes(b"")
    (out_dir / "genome_peaks.bin").write_bytes(b"")
    (out_dir / "meta.bin").write_bytes(b"")
