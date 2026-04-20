from __future__ import annotations

import pickle
import struct
from pathlib import Path

from tools.pickle_to_mmap.format import HeaderLayout


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


def _write_meta(
    out_dir: Path,
    all_peaks: dict[str, list[float]],
    genes: dict[str, int],
    names: dict[str, str],
    taxonomy: dict[str, dict[str, object] | str],
    genome_ids: list[str],
) -> None:
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

    (out_dir / "meta.bin").write_bytes(bytes(payload))


def convert_fixture(source_dir: Path, out_dir: Path) -> None:
    source_dir = Path(source_dir)
    out_dir = Path(out_dir)

    all_peaks = _load_pickle(source_dir / "all.db")
    genes = _load_pickle(source_dir / "genes.db")
    names = _load_pickle(source_dir / "names.db")
    taxonomy = _load_pickle(source_dir / "taxonomy.db")
    genome_ids = sorted(all_peaks)

    out_dir.mkdir(parents=True, exist_ok=True)

    # The Task 4 skeleton only guarantees artifact creation.
    (out_dir / "header.bin").write_bytes(HeaderLayout.MAGIC.ljust(HeaderLayout.SIZE, b"\0"))
    (out_dir / "mass_index.bin").write_bytes(b"")
    (out_dir / "genome_peaks.bin").write_bytes(b"")
    _write_meta(out_dir, all_peaks, genes, names, taxonomy, genome_ids)
