from __future__ import annotations

from dataclasses import dataclass
import struct
import zlib


@dataclass(frozen=True)
class HeaderLayout:
    MAGIC = b"GPMDB\0\1"
    SIZE = 256
    SCHEMA_VERSION = 1
    DEFAULT_BIN_WIDTH_MILLI_MZ = 100
    CRC32_HEADER_OFFSET = 80
    STRUCT = struct.Struct("<8sIIQQQQQQQQIIII160s")


def crc32_bytes(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF
