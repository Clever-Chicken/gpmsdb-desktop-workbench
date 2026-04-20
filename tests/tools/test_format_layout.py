from tools.pickle_to_mmap.format import HeaderLayout


def test_header_size_is_fixed() -> None:
    assert HeaderLayout.SIZE == 256


def test_magic_prefix_is_stable() -> None:
    assert HeaderLayout.MAGIC == b"GPMDB\0\1"
