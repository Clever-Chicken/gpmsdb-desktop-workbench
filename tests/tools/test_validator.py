from pathlib import Path

from tests.fixtures.small_source.build_fixture import build_fixture
from tools.pickle_to_mmap.convert_gpmsdb import convert_fixture
from tools.pickle_to_mmap.validate_gpmsdb import validate_output_dir


def test_validator_rejects_missing_meta(tmp_path: Path) -> None:
    (tmp_path / "header.bin").write_bytes(b"header")
    (tmp_path / "mass_index.bin").write_bytes(b"index")
    (tmp_path / "genome_peaks.bin").write_bytes(b"peaks")

    ok, errors = validate_output_dir(tmp_path)
    assert ok is False
    assert "meta.bin is missing" in errors


def test_validator_accepts_dictionary_coded_meta(tmp_path: Path) -> None:
    src = tmp_path / "source"
    build_fixture(src)

    out = tmp_path / "out"
    convert_fixture(src, out)

    ok, errors = validate_output_dir(out)
    assert ok is True
    assert errors == []


def test_validator_rejects_corrupt_header_size(tmp_path: Path) -> None:
    src = tmp_path / "source"
    build_fixture(src)

    out = tmp_path / "out"
    convert_fixture(src, out)
    (out / "header.bin").write_bytes(b"short")

    ok, errors = validate_output_dir(out)
    assert ok is False
    assert "header.bin size mismatch" in errors
