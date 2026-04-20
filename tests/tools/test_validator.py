from pathlib import Path

from tools.pickle_to_mmap.validate_gpmsdb import validate_output_dir


def test_validator_rejects_missing_meta(tmp_path: Path) -> None:
    (tmp_path / "header.bin").write_bytes(b"header")
    (tmp_path / "mass_index.bin").write_bytes(b"index")
    (tmp_path / "genome_peaks.bin").write_bytes(b"peaks")

    ok, errors = validate_output_dir(tmp_path)
    assert ok is False
    assert "meta.bin is missing" in errors
