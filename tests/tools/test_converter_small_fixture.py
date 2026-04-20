import pickle
from pathlib import Path

from tools.pickle_to_mmap.convert_gpmsdb import convert_fixture


def test_converter_emits_all_artifacts(tmp_path: Path) -> None:
    src = tmp_path / "source"
    src.mkdir()

    for name in ["ribosomal.db", "all.db", "genes.db", "names.db", "taxonomy.db"]:
        with (src / name).open("wb") as f:
            pickle.dump({"ok": True}, f, protocol=pickle.HIGHEST_PROTOCOL)

    out = tmp_path / "out"
    convert_fixture(src, out)

    assert (out / "header.bin").exists()
    assert (out / "mass_index.bin").exists()
    assert (out / "genome_peaks.bin").exists()
    assert (out / "meta.bin").exists()
