from __future__ import annotations

import argparse
import pickle
import struct
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.pickle_to_mmap.binning import encode_peak_value, mass_to_bin
from tools.pickle_to_mmap.format import HeaderLayout, crc32_bytes


def _load_pickle(path: Path) -> object:
    with path.open("rb") as handle:
        return pickle.load(handle)


def _encode_string_dictionary(values: list[str]) -> tuple[tuple[int, ...], bytes, tuple[int, ...]]:
    dictionary: list[bytes] = [b""]
    value_to_id: dict[bytes, int] = {b"": 0}
    ids: list[int] = []

    for value in values:
        raw = value.encode("utf-8")
        dict_id = value_to_id.get(raw)
        if dict_id is None:
            dict_id = len(dictionary)
            dictionary.append(raw)
            value_to_id[raw] = dict_id
        ids.append(dict_id)

    offsets = [0]
    blob = bytearray()
    for raw in dictionary:
        blob.extend(raw)
        offsets.append(len(blob))

    return tuple(offsets), bytes(blob), tuple(ids)


def _collect_metadata(
    all_peaks: dict[str, list[float]],
    genes: dict[str, int],
    names: dict[str, str],
    taxonomy: dict[str, dict[str, object] | str],
    genome_ids: list[str],
) -> tuple[list[int], list[int], list[int], list[str], list[str]]:
    genome_offsets = [0]
    running_total = 0
    gene_counts: list[int] = []
    taxonomy_ids: list[int] = []
    genome_names: list[str] = []
    genome_taxonomy_text: list[str] = []

    for genome_id in genome_ids:
        peaks = all_peaks[genome_id]
        running_total += len(peaks)
        genome_offsets.append(running_total)
        gene_counts.append(int(genes[genome_id]))
        genome_names.append(str(names[genome_id]))

        tax_entry = taxonomy[genome_id]
        if isinstance(tax_entry, dict):
            taxonomy_ids.append(int(tax_entry.get("id", 0)))
            genome_taxonomy_text.append(str(tax_entry.get("text", "")))
        else:
            taxonomy_ids.append(0)
            genome_taxonomy_text.append(str(tax_entry))

    return genome_offsets, gene_counts, taxonomy_ids, genome_names, genome_taxonomy_text


def _build_meta_bytes(
    all_peaks: dict[str, list[float]],
    genes: dict[str, int],
    names: dict[str, str],
    taxonomy: dict[str, dict[str, object] | str],
    genome_ids: list[str],
) -> bytes:
    genome_offsets, gene_counts, taxonomy_ids, genome_names, genome_taxonomy_text = _collect_metadata(
        all_peaks, genes, names, taxonomy, genome_ids
    )

    name_offsets, name_blob, genome_name_dict_ids = _encode_string_dictionary(genome_names)
    taxonomy_offsets, taxonomy_blob, genome_taxonomy_dict_ids = _encode_string_dictionary(genome_taxonomy_text)

    payload = bytearray()
    payload.extend(struct.pack(f"<{len(genome_offsets)}Q", *genome_offsets))
    payload.extend(struct.pack(f"<{len(gene_counts)}I", *gene_counts))
    payload.extend(struct.pack(f"<{len(taxonomy_ids)}I", *taxonomy_ids))
    payload.extend(struct.pack("<I", len(name_offsets) - 1))
    payload.extend(struct.pack("<I", len(taxonomy_offsets) - 1))
    payload.extend(struct.pack(f"<{len(name_offsets)}Q", *name_offsets))
    payload.extend(struct.pack(f"<{len(taxonomy_offsets)}Q", *taxonomy_offsets))
    payload.extend(struct.pack(f"<{len(genome_name_dict_ids)}I", *genome_name_dict_ids))
    payload.extend(struct.pack(f"<{len(genome_taxonomy_dict_ids)}I", *genome_taxonomy_dict_ids))
    payload.extend(struct.pack("<Q", len(name_blob)))
    payload.extend(struct.pack("<Q", len(taxonomy_blob)))
    payload.extend(name_blob)
    payload.extend(taxonomy_blob)
    return bytes(payload)


def _build_genome_peaks_bytes(all_peaks: dict[str, list[float]], genome_ids: list[str]) -> tuple[bytes, list[list[int]]]:
    encoded_per_genome: list[list[int]] = []
    flattened: list[int] = []

    for genome_id in genome_ids:
        encoded = [encode_peak_value(mz) for mz in all_peaks[genome_id]]
        encoded_per_genome.append(encoded)
        flattened.extend(encoded)

    payload = struct.pack(f"<{len(flattened)}I", *flattened) if flattened else b""
    return payload, encoded_per_genome


def _build_mass_index_bytes(
    encoded_per_genome: list[list[int]], bin_width_milli_mz: int
) -> bytes:
    bins: dict[int, list[tuple[int, int]]] = {}
    max_bin = 0

    for genome_id, peaks in enumerate(encoded_per_genome):
        for local_peak_idx, peak_value in enumerate(peaks):
            bin_id = mass_to_bin(peak_value, bin_width_milli_mz)
            bins.setdefault(bin_id, []).append((genome_id, local_peak_idx))
            if bin_id > max_bin:
                max_bin = bin_id

    bin_count = max_bin + 1 if bins else 1
    offsets: list[int] = []
    postings: list[tuple[int, int]] = []

    for bin_id in range(bin_count):
        offsets.append(len(postings))
        postings.extend(sorted(bins.get(bin_id, ())))
    offsets.append(len(postings))

    payload = bytearray()
    payload.extend(struct.pack("<I", bin_count))
    payload.extend(struct.pack(f"<{len(offsets)}Q", *offsets))
    for genome_id, local_peak_idx in postings:
        payload.extend(struct.pack("<II", genome_id, local_peak_idx))

    return bytes(payload)


def _build_header_bytes(
    *,
    bin_width_milli_mz: int,
    genome_count: int,
    total_peak_count: int,
    mass_index_bytes: bytes,
    genome_peaks_bytes: bytes,
    meta_bytes: bytes,
) -> bytes:
    mass_index_offset = HeaderLayout.SIZE
    mass_index_len = len(mass_index_bytes)
    genome_peaks_offset = mass_index_offset + mass_index_len
    genome_peaks_len = len(genome_peaks_bytes)
    meta_offset = genome_peaks_offset + genome_peaks_len
    meta_len = len(meta_bytes)

    reserved = bytes(160)
    header_without_crc = HeaderLayout.STRUCT.pack(
        HeaderLayout.MAGIC.ljust(8, b"\0"),
        HeaderLayout.SCHEMA_VERSION,
        bin_width_milli_mz,
        genome_count,
        total_peak_count,
        mass_index_offset,
        mass_index_len,
        genome_peaks_offset,
        genome_peaks_len,
        meta_offset,
        meta_len,
        0,
        crc32_bytes(mass_index_bytes),
        crc32_bytes(genome_peaks_bytes),
        crc32_bytes(meta_bytes),
        reserved,
    )
    crc32_header = crc32_bytes(header_without_crc)

    return HeaderLayout.STRUCT.pack(
        HeaderLayout.MAGIC.ljust(8, b"\0"),
        HeaderLayout.SCHEMA_VERSION,
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
        crc32_bytes(mass_index_bytes),
        crc32_bytes(genome_peaks_bytes),
        crc32_bytes(meta_bytes),
        reserved,
    )


def convert_fixture(
    source_dir: Path,
    out_dir: Path,
    *,
    bin_width_milli_mz: int = HeaderLayout.DEFAULT_BIN_WIDTH_MILLI_MZ,
) -> None:
    source_dir = Path(source_dir)
    out_dir = Path(out_dir)

    all_peaks = _load_pickle(source_dir / "all.db")
    genes = _load_pickle(source_dir / "genes.db")
    names = _load_pickle(source_dir / "names.db")
    taxonomy = _load_pickle(source_dir / "taxonomy.db")
    genome_ids = sorted(all_peaks)

    genome_peaks_bytes, encoded_per_genome = _build_genome_peaks_bytes(all_peaks, genome_ids)
    meta_bytes = _build_meta_bytes(all_peaks, genes, names, taxonomy, genome_ids)
    mass_index_bytes = _build_mass_index_bytes(encoded_per_genome, bin_width_milli_mz)
    header_bytes = _build_header_bytes(
        bin_width_milli_mz=bin_width_milli_mz,
        genome_count=len(genome_ids),
        total_peak_count=sum(len(peaks) for peaks in encoded_per_genome),
        mass_index_bytes=mass_index_bytes,
        genome_peaks_bytes=genome_peaks_bytes,
        meta_bytes=meta_bytes,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "header.bin").write_bytes(header_bytes)
    (out_dir / "mass_index.bin").write_bytes(mass_index_bytes)
    (out_dir / "genome_peaks.bin").write_bytes(genome_peaks_bytes)
    (out_dir / "meta.bin").write_bytes(meta_bytes)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument(
        "--bin-width-milli-mz",
        type=int,
        default=HeaderLayout.DEFAULT_BIN_WIDTH_MILLI_MZ,
    )
    args = parser.parse_args()

    convert_fixture(
        args.source_dir,
        args.out_dir,
        bin_width_milli_mz=args.bin_width_milli_mz,
    )


if __name__ == "__main__":
    main()
