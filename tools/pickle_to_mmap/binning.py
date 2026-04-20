from __future__ import annotations


def encode_peak_value(mz: float) -> int:
    if mz < 0:
        raise ValueError("mz must be non-negative")
    return int((mz * 1000.0) + 0.5)


def mass_to_bin(peak_value: int, bin_width_milli_mz: int) -> int:
    if bin_width_milli_mz <= 0:
        raise ValueError("bin width must be positive")
    return peak_value // bin_width_milli_mz
