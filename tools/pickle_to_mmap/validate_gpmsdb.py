from __future__ import annotations

import struct
from pathlib import Path

from tools.pickle_to_mmap.format import HeaderLayout, crc32_bytes


def _check_required_files(out_dir: Path, errors: list[str]) -> bool:
    ok = True
    for filename in ("header.bin", "mass_index.bin", "genome_peaks.bin", "meta.bin"):
        if not (out_dir / filename).exists():
            errors.append(f"{filename} is missing")
            ok = False
    return ok


def validate_output_dir(out_dir: Path) -> tuple[bool, list[str]]:
    out_dir = Path(out_dir)
    errors: list[str] = []

    if not _check_required_files(out_dir, errors):
        return False, errors

    header_path = out_dir / "header.bin"
    mass_index_path = out_dir / "mass_index.bin"
    genome_peaks_path = out_dir / "genome_peaks.bin"
    meta_path = out_dir / "meta.bin"

    header_raw = header_path.read_bytes()
    mass_index_raw = mass_index_path.read_bytes()
    genome_peaks_raw = genome_peaks_path.read_bytes()
    meta_raw = meta_path.read_bytes()

    if len(header_raw) != HeaderLayout.SIZE:
        errors.append("header.bin size mismatch")
        return False, errors

    if meta_path.stat().st_size == 0:
        errors.append("meta.bin is empty")

    unpacked = HeaderLayout.STRUCT.unpack(header_raw)
    (
        magic,
        schema_version,
        bin_width_milli_mz,
        genome_count,
        total_peak_count,
        mass_index_offset,
        mass_index_len,
        genome_peaks_offset,
        genome_peaks_len,
        meta_offset,
        meta_len,
        crc32_header,
        crc32_mass_index,
        crc32_genome_peaks,
        crc32_meta,
        reserved,
    ) = unpacked

    if magic.rstrip(b"\0") != HeaderLayout.MAGIC:
        errors.append("header magic mismatch")
    if schema_version != HeaderLayout.SCHEMA_VERSION:
        errors.append("schema version mismatch")
    if reserved != bytes(len(reserved)):
        errors.append("header reserved bytes must be zero")
    if bin_width_milli_mz <= 0:
        errors.append("bin width must be positive")

    if mass_index_offset != HeaderLayout.SIZE:
        errors.append("mass_index offset mismatch")
    if mass_index_len != len(mass_index_raw):
        errors.append("mass_index length mismatch")
    if genome_peaks_offset != HeaderLayout.SIZE + len(mass_index_raw):
        errors.append("genome_peaks offset mismatch")
    if genome_peaks_len != len(genome_peaks_raw):
        errors.append("genome_peaks length mismatch")
    if meta_offset != genome_peaks_offset + len(genome_peaks_raw):
        errors.append("meta offset mismatch")
    if meta_len != len(meta_raw):
        errors.append("meta length mismatch")
    if genome_peaks_len != total_peak_count * 4:
        errors.append("genome_peaks.bin size does not match total_peak_count")

    header_for_crc = bytearray(header_raw)
    header_for_crc[HeaderLayout.CRC32_HEADER_OFFSET:HeaderLayout.CRC32_HEADER_OFFSET + 4] = b"\0\0\0\0"
    if crc32_header != crc32_bytes(bytes(header_for_crc)):
        errors.append("header crc mismatch")
    if crc32_mass_index != crc32_bytes(mass_index_raw):
        errors.append("mass_index crc mismatch")
    if crc32_genome_peaks != crc32_bytes(genome_peaks_raw):
        errors.append("genome_peaks crc mismatch")
    if crc32_meta != crc32_bytes(meta_raw):
        errors.append("meta crc mismatch")

    if len(mass_index_raw) >= 4:
        bin_count = struct.unpack_from("<I", mass_index_raw, 0)[0]
        expected_offset_bytes = 4 + (bin_count + 1) * 8
        if len(mass_index_raw) < expected_offset_bytes:
            errors.append("mass_index.bin is too small")
        else:
            offsets = struct.unpack_from(f"<{bin_count + 1}Q", mass_index_raw, 4)
            posting_count = offsets[-1]
            expected_mass_index_len = expected_offset_bytes + posting_count * 8
            if expected_mass_index_len != len(mass_index_raw):
                errors.append("mass_index structural length mismatch")
    else:
        errors.append("mass_index.bin is too small")

    return (len(errors) == 0, errors)
