from __future__ import annotations

from pathlib import Path


def validate_output_dir(out_dir: Path) -> tuple[bool, list[str]]:
    out_dir = Path(out_dir)
    errors: list[str] = []

    for filename in ("header.bin", "mass_index.bin", "genome_peaks.bin", "meta.bin"):
        if not (out_dir / filename).exists():
            errors.append(f"{filename} is missing")

    meta_path = out_dir / "meta.bin"
    if meta_path.exists() and meta_path.stat().st_size == 0:
        errors.append("meta.bin is empty")

    return (len(errors) == 0, errors)
