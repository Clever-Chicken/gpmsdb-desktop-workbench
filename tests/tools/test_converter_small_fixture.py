import struct
from pathlib import Path

from tests.fixtures.small_source.build_fixture import build_fixture
from tools.pickle_to_mmap.convert_gpmsdb import convert_fixture


def test_converter_emits_all_artifacts(tmp_path: Path) -> None:
    src = tmp_path / "source"
    build_fixture(src)
    out = tmp_path / "out"
    convert_fixture(src, out)

    assert (out / "header.bin").exists()
    assert (out / "mass_index.bin").exists()
    assert (out / "genome_peaks.bin").exists()
    assert (out / "meta.bin").exists()


def _read_u64_array(raw: bytes, offset: int, count: int) -> tuple[tuple[int, ...], int]:
    size = 8 * count
    values = struct.unpack_from(f"<{count}Q", raw, offset)
    return values, offset + size


def _read_u32_array(raw: bytes, offset: int, count: int) -> tuple[tuple[int, ...], int]:
    size = 4 * count
    values = struct.unpack_from(f"<{count}I", raw, offset)
    return values, offset + size


def _decode_dict(blob: bytes, offsets: tuple[int, ...]) -> list[str]:
    decoded: list[str] = []
    for start, end in zip(offsets, offsets[1:]):
        decoded.append(blob[start:end].decode("utf-8"))
    return decoded


def test_converter_writes_dictionary_coded_meta(tmp_path: Path) -> None:
    src = tmp_path / "source"
    build_fixture(src)

    out = tmp_path / "out"
    convert_fixture(src, out)

    raw = (out / "meta.bin").read_bytes()
    offset = 0

    genome_offsets, offset = _read_u64_array(raw, offset, 3)
    gene_counts, offset = _read_u32_array(raw, offset, 2)
    taxonomy_ids, offset = _read_u32_array(raw, offset, 2)
    name_dict_count = struct.unpack_from("<I", raw, offset)[0]
    offset += 4
    taxonomy_dict_count = struct.unpack_from("<I", raw, offset)[0]
    offset += 4
    name_dict_offsets, offset = _read_u64_array(raw, offset, name_dict_count + 1)
    taxonomy_dict_offsets, offset = _read_u64_array(raw, offset, taxonomy_dict_count + 1)
    genome_name_dict_ids, offset = _read_u32_array(raw, offset, 2)
    genome_taxonomy_dict_ids, offset = _read_u32_array(raw, offset, 2)
    name_blob_len = struct.unpack_from("<Q", raw, offset)[0]
    offset += 8
    taxonomy_blob_len = struct.unpack_from("<Q", raw, offset)[0]
    offset += 8
    name_blob = raw[offset:offset + name_blob_len]
    offset += name_blob_len
    taxonomy_blob = raw[offset:offset + taxonomy_blob_len]

    assert genome_offsets == (0, 2, 4)
    assert gene_counts == (900, 1100)
    assert taxonomy_ids == (101, 101)
    assert name_dict_count == 3
    assert taxonomy_dict_count == 2
    assert genome_name_dict_ids == (1, 2)
    assert genome_taxonomy_dict_ids == (1, 1)
    assert _decode_dict(name_blob, name_dict_offsets) == ["", "Genome Zero", "Genome One"]
    assert _decode_dict(taxonomy_blob, taxonomy_dict_offsets) == ["", "d__Bacteria;p__Shared"]
