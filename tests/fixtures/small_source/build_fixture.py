from __future__ import annotations

import pickle
from pathlib import Path


FIXTURE_FILES = {
    "ribosomal.db": {"RPSA": [101.1, 202.2]},
    "all.db": {"P00001": {"mass": 51234.5, "organism": 9606}},
    "genes.db": {"geneA": {"id": 1, "name": "geneA"}},
    "names.db": {9606: "Homo sapiens"},
    "taxonomy.db": {9606: {"parent": 9605, "rank": "species"}},
}


def build_fixture(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, payload in FIXTURE_FILES.items():
        with (output_dir / filename).open("wb") as f:
            pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == "__main__":
    build_fixture(Path(__file__).resolve().parent)
