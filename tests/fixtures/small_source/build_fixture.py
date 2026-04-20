from __future__ import annotations

import pickle
from pathlib import Path


FIXTURE_FILES = {
    "ribosomal.db": {
        "g0": [1000.0, 1500.0],
        "g1": [1001.0, 1800.0],
    },
    "all.db": {
        "g0": [1000.0, 1200.0],
        "g1": [1001.0, 1400.0],
    },
    "genes.db": {
        "g0": 900,
        "g1": 1100,
    },
    "names.db": {
        "g0": "Genome Zero",
        "g1": "Genome One",
    },
    "taxonomy.db": {
        "g0": {"id": 101, "text": "d__Bacteria;p__Shared"},
        "g1": {"id": 101, "text": "d__Bacteria;p__Shared"},
    },
}


def build_fixture(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, payload in FIXTURE_FILES.items():
        with (output_dir / filename).open("wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    build_fixture(Path(__file__).resolve().parent)
