# GPMsDB Binary Database Format (Frozen, Task 2 Aligned)

## 1. Scope and Normative Rules

This document defines the frozen runtime binary format used by GPMsDB.

- Runtime artifacts are exactly 4 files: `header.bin`, `mass_index.bin`, `genome_peaks.bin`, `meta.bin`.
- All integer fields are little-endian.
- Record IDs are zero-based.
- Runtime reads these files via memory mapping and does not heap-deserialize the full dataset.
- Any field below that is not in the Task 2 example is included only when strictly required for parseability; it is a minimal supplement, not extra feature design.

## 2. Primitive Types

- `u8`: 1 byte
- `u32`: 4 bytes
- `u64`: 8 bytes
- `posting`: packed pair `(u32 genome_id, u32 local_peak_idx)`, total 8 bytes

## 3. Logical File Order and Offsets

Header offsets are logical offsets in this virtual concatenation:

1. `header.bin`
2. `mass_index.bin`
3. `genome_peaks.bin`
4. `meta.bin`

Logical offset `0` is the first byte of `header.bin`.

## 4. header.bin

`header.bin` is fixed-size 256 bytes.

### 4.1 Byte Layout

| Offset | Size | Type | Name | Meaning |
|---:|---:|---|---|---|
| 0 | 8 | `u8[8]` | `magic` | Literal `"GPMDB\0\1"`; stored as 8 bytes with final zero pad. Exact bytes: `47 50 4D 44 42 00 01 00` |
| 8 | 4 | `u32` | `schema_version` | Current frozen value `1` |
| 12 | 4 | `u32` | `bin_width_milli_mz` | Bin width in milli-m/z units |
| 16 | 8 | `u64` | `genome_count` | Number of genomes |
| 24 | 8 | `u64` | `total_peak_count` | Total peak count across all genomes |
| 32 | 8 | `u64` | `mass_index_offset` | Logical offset of `mass_index.bin` |
| 40 | 8 | `u64` | `mass_index_len` | Byte length of `mass_index.bin` |
| 48 | 8 | `u64` | `genome_peaks_offset` | Logical offset of `genome_peaks.bin` |
| 56 | 8 | `u64` | `genome_peaks_len` | Byte length of `genome_peaks.bin` |
| 64 | 8 | `u64` | `meta_offset` | Logical offset of `meta.bin` |
| 72 | 8 | `u64` | `meta_len` | Byte length of `meta.bin` |
| 80 | 4 | `u32` | `crc32_header` | CRC32 of `header.bin` (see 4.2) |
| 84 | 4 | `u32` | `crc32_mass_index` | CRC32 of entire `mass_index.bin` |
| 88 | 4 | `u32` | `crc32_genome_peaks` | CRC32 of entire `genome_peaks.bin` |
| 92 | 4 | `u32` | `crc32_meta` | CRC32 of entire `meta.bin` |
| 96 | 160 | `u8[160]` | `reserved` | Zero-filled reserved bytes |

### 4.2 CRC32 Semantics

- CRC algorithm: IEEE CRC-32 (`poly=0x04C11DB7`, reflected `0xEDB88320`, init `0xFFFFFFFF`, xorout `0xFFFFFFFF`).
- `crc32_mass_index`, `crc32_genome_peaks`, `crc32_meta`: CRC of full target file bytes.
- `crc32_header`: CRC of all 256 bytes of `header.bin`, with bytes `[80,84)` treated as zero during CRC calculation.

## 5. genome_peaks.bin

Physical layout:

- `peak_values[0..total_peak_count-1]` as contiguous `u32`.

Encoding and size:

- `peak_value = round(mz * 1000.0)`.
- `genome_peaks_len = total_peak_count * 4`.

Per-genome slices are defined by `meta.bin::genome_offsets`:

- genome `g` uses global peak range `[genome_offsets[g], genome_offsets[g+1])`.

## 6. mass_index.bin

### 6.1 Physical Layout

`mass_index.bin` keeps the Task 2主体结构 `bin_offsets + postings`, with one minimal parseability prefix:

1. `bin_count` as `u32` at file offset 0  
   (minimal prefix so `(bin_count + 1)` offsets are parseable; does not change the `bin_offsets + postings`主体结构)
2. `bin_offsets[0..bin_count]` as `u64` (`bin_count + 1` entries)
3. `postings[0..bin_offsets[bin_count]-1]` as packed `posting` (8 bytes each)

Derived lengths:

- `bin_offsets_bytes = (bin_count + 1) * 8`
- `postings_bytes = bin_offsets[bin_count] * 8`
- `mass_index_len = 4 + bin_offsets_bytes + postings_bytes`

### 6.2 Bin and Posting Semantics

- `bin_id = floor(peak_value / bin_width_milli_mz)`.
- Valid `bin_id` range: `[0, bin_count)`.
- Posting slice for bin `b`: indices `[bin_offsets[b], bin_offsets[b+1])`.
- Posting fields:
  - `genome_id` in `[0, genome_count)`
  - `local_peak_idx` in owning genome local range

Local-to-global mapping:

- `global_peak_idx = genome_offsets[genome_id] + local_peak_idx`
- Must satisfy `global_peak_idx < genome_offsets[genome_id + 1]`

## 7. meta.bin

### 7.1 Physical Layout (Dictionary-Coded UTF-8 Blobs, Frozen)

`meta.bin` stores fixed-width genome arrays plus two deduplicated string dictionaries (`name`, `taxonomy`).
Physical order is exact and fixed:

1. `genome_offsets[0..genome_count]` as `u64` (`genome_count + 1` entries)
2. `gene_counts[0..genome_count-1]` as `u32`
3. `taxonomy_ids[0..genome_count-1]` as `u32`
4. `name_dict_count` as `u32`
5. `taxonomy_dict_count` as `u32`
6. `name_dict_offsets[0..name_dict_count]` as `u64` (`name_dict_count + 1` entries)
7. `taxonomy_dict_offsets[0..taxonomy_dict_count]` as `u64` (`taxonomy_dict_count + 1` entries)
8. `genome_name_dict_ids[0..genome_count-1]` as `u32`
9. `genome_taxonomy_dict_ids[0..genome_count-1]` as `u32`
10. `name_blob_len` as `u64`
11. `taxonomy_blob_len` as `u64`
12. `name_blob[0..name_blob_len-1]` as raw bytes
13. `taxonomy_blob[0..taxonomy_blob_len-1]` as raw bytes

Derived byte sizes:

- `genome_offsets_bytes = (genome_count + 1) * 8`
- `gene_counts_bytes = genome_count * 4`
- `taxonomy_ids_bytes = genome_count * 4`
- `name_dict_offsets_bytes = (name_dict_count + 1) * 8`
- `taxonomy_dict_offsets_bytes = (taxonomy_dict_count + 1) * 8`
- `genome_name_dict_ids_bytes = genome_count * 4`
- `genome_taxonomy_dict_ids_bytes = genome_count * 4`
- `meta_len = genome_offsets_bytes + gene_counts_bytes + taxonomy_ids_bytes + 4 + 4 + name_dict_offsets_bytes + taxonomy_dict_offsets_bytes + genome_name_dict_ids_bytes + genome_taxonomy_dict_ids_bytes + 8 + 8 + name_blob_len + taxonomy_blob_len`

### 7.2 Dictionary Coding Rules

- Dictionaries are per-file: one for genome names, one for taxonomy strings.
- Deduplication is byte-exact over UTF-8 bytes; identical byte sequence maps to one dictionary entry.
- `dict_id` is zero-based.
- `dict_id = 0` is reserved for empty string (`""`) in both dictionaries.
- Therefore, `name_dict_count >= 1` and `taxonomy_dict_count >= 1` always hold.
- Dictionary entry payload is stored in blob slices defined by offset tables, not by delimiters.

Offset-table slicing rule:

- Entry `i` in `name` dictionary is bytes `name_blob[name_dict_offsets[i] .. name_dict_offsets[i+1])`.
- Entry `i` in `taxonomy` dictionary is bytes `taxonomy_blob[taxonomy_dict_offsets[i] .. taxonomy_dict_offsets[i+1])`.
- No `NUL` delimiter or any other separator is used inside blobs.

### 7.3 Genome Row Decode Rule (Reader)

For genome row `g`:

1. Read `name_id = genome_name_dict_ids[g]`.
2. Read `taxonomy_id = genome_taxonomy_dict_ids[g]`.
3. Slice `name` bytes from `name_blob` via `name_dict_offsets[name_id .. name_id+1]`.
4. Slice `taxonomy` bytes from `taxonomy_blob` via `taxonomy_dict_offsets[taxonomy_id .. taxonomy_id+1]`.
5. Validate both slices as UTF-8 and materialize strings only if needed by caller.

This preserves mmap-friendly access: fixed-width arrays for row lookup, deduplicated blobs for high cache locality and low memory pressure.

## 8. Runtime Invariants (Mandatory)

Readers must reject dataset if any check fails:

1. `magic` mismatch.
2. `schema_version != 1`.
3. Any file length differs from header `*_len`.
4. Offsets monotonic and bounded: `256 <= mass_index_offset <= genome_peaks_offset <= meta_offset`, and each section end does not exceed logical total length `256 + mass_index_len + genome_peaks_len + meta_len`.
5. `mass_index_offset = 256`.
6. `genome_peaks_offset = mass_index_offset + mass_index_len`.
7. `meta_offset = genome_peaks_offset + genome_peaks_len`.
8. `bin_width_milli_mz > 0`.
9. `bin_count > 0`.
10. `bin_offsets[0] = 0` and `bin_offsets` monotonic non-decreasing.
11. `bin_offsets[bin_count] * 8 = postings_bytes`.
12. `genome_offsets[0] = 0`.
13. `genome_offsets[genome_count] = total_peak_count`.
14. `genome_peaks_len = total_peak_count * 4`.
15. Every posting has `genome_id < genome_count`.
16. Every posting has `local_peak_idx < (genome_offsets[genome_id+1] - genome_offsets[genome_id])`.
17. Every derived `global_peak_idx` is within `[0, total_peak_count)`.
18. `name_dict_count >= 1`, `taxonomy_dict_count >= 1`, and entry `0` in both dictionaries is exactly empty string (`offsets[0] = 0` and `offsets[1] = 0`).
19. `name_dict_offsets[0] = 0`, `taxonomy_dict_offsets[0] = 0`; both offset arrays are monotonic non-decreasing.
20. `name_dict_offsets[name_dict_count] = name_blob_len` and `taxonomy_dict_offsets[taxonomy_dict_count] = taxonomy_blob_len`.
21. Every `genome_name_dict_ids[g] < name_dict_count` and every `genome_taxonomy_dict_ids[g] < taxonomy_dict_count`.
22. Every dictionary slice decoded via offset tables is valid UTF-8 (no invalid byte sequences).
23. CRC checks (section 4.2) all pass.

## 9. Version Gate

- Current format is `schema_version = 1`.
- Any other `schema_version` must be rejected.
