from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HeaderLayout:
    MAGIC = b"GPMDB\0\1"
    SIZE = 256

