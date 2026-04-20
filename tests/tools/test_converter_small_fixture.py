import struct
from pathlib import Path

from tests.fixtures.small_source.build_fixture import build_fixture
from tools.pickle_to_mmap.convert_gpmsdb import convert_fixture
from tools.pickle_to_mmap.format import HeaderLayout


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


def test_converter_writes_packed_genome_peaks(tmp_path: Path) -> None:
    src = tmp_path / "source"
    build_fixture(src)

    out = tmp_path / "out"
    convert_fixture(src, out)

    raw = (out / "genome_peaks.bin").read_bytes()
    peaks = struct.unpack("<4I", raw)
    assert peaks == (1_000_000, 1_200_000, 1_001_000, 1_400_000)


def test_converter_writes_real_header_with_crc_and_lengths(tmp_path: Path) -> None:
    src = tmp_path / "source"
    build_fixture(src)

    out = tmp_path / "out"
    convert_fixture(src, out)

    raw = (out / "header.bin").read_bytes()
    assert len(raw) == HeaderLayout.SIZE
    assert raw[: len(HeaderLayout.MAGIC)] == HeaderLayout.MAGIC

    schema_version = struct.unpack_from("<I", raw, 8)[0]
    bin_width = struct.unpack_from("<I", raw, 12)[0]
    genome_count = struct.unpack_from("<Q", raw, 16)[0]
    total_peak_count = struct.unpack_from("<Q", raw, 24)[0]
    mass_index_offset = struct.unpack_from("<Q", raw, 32)[0]
    mass_index_len = struct.unpack_from("<Q", raw, 40)[0]
    genome_peaks_offset = struct.unpack_from("<Q", raw, 48)[0]
    genome_peaks_len = struct.unpack_from("<Q", raw, 56)[0]
    meta_offset = struct.unpack_from("<Q", raw, 64)[0]
    meta_len = struct.unpack_from("<Q", raw, 72)[0]
    crc32_mass_index = struct.unpack_from("<I", raw, 84)[0]
    crc32_genome_peaks = struct.unpack_from("<I", raw, 88)[0]
    crc32_meta = struct.unpack_from("<I", raw, 92)[0]

    assert schema_version == 1
    assert bin_width == 100
    assert genome_count == 2
    assert total_peak_count == 4
    assert mass_index_offset == HeaderLayout.SIZE
    assert mass_index_len == (out / "mass_index.bin").stat().st_size
    assert genome_peaks_offset == HeaderLayout.SIZE + mass_index_len
    assert genome_peaks_len == (out / "genome_peaks.bin").stat().st_size
    assert meta_offset == genome_peaks_offset + genome_peaks_len
    assert meta_len == (out / "meta.bin").stat().st_size
    assert crc32_mass_index != 0
    assert crc32_genome_peaks != 0
    assert crc32_meta != 0


def test_converter_writes_mass_index_postings(tmp_path: Path) -> None:
    src = tmp_path / "source"
    build_fixture(src)

    out = tmp_path / "out"
    convert_fixture(src, out)

    raw = (out / "mass_index.bin").read_bytes()
    bin_count = struct.unpack_from("<I", raw, 0)[0]
    offsets = struct.unpack_from(f"<{bin_count + 1}Q", raw, 4)
    postings_offset = 4 + (bin_count + 1) * 8
    posting_count = offsets[-1]
    postings = [
        struct.unpack_from("<II", raw, postings_offset + idx * 8)
        for idx in range(posting_count)
    ]

    assert bin_count == 14_001
    assert posting_count == 4
    assert postings == [(0, 0), (1, 0), (0, 1), (1, 1)]
    assert offsets[10_000] == 0
    assert offsets[10_001] == 1
    assert offsets[10_010] == 1
    assert offsets[12_000] == 2
    assert offsets[14_000] == 3
    assert offsets[14_001] == 4
