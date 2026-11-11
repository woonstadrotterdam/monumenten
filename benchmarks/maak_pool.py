# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow>=15"]
# ///
"""Maak de pools met verblijfsobject-ID's waaruit de snelheidsmeting steekproeven trekt.

De ID's komen uit een vastgepinde versie van de dataset
https://huggingface.co/datasets/woonstadrotterdam/monumenten. De monumentvlaggen in
die dataset dienen alleen om een monumentrijke pool te kiezen, niet als waarheid.

Gebruik (downloadt eenmalig ongeveer 150 MB):
    uv run benchmarks/maak_pool.py
"""

import gzip
import random
import tempfile
import urllib.request
from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-not-found]

HF_REVISIE = "21def8bcd02af664670d336c03ba598e84ba42c2"  # tag 2026-03-10
HF_URL = (
    "https://huggingface.co/datasets/woonstadrotterdam/monumenten/resolve/"
    f"{HF_REVISIE}/monumenten.parquet"
)
POOL_GROOTTE = 200_000
SEED = 2026
POOL_MAP = Path(__file__).parent / "pool"


def schrijf_pool(naam: str, ids: list[str]) -> None:
    """Schrijf gesorteerde ID's naar een gzip-bestand dat bij elke run byte-gelijk is."""
    pad = POOL_MAP / f"{naam}.txt.gz"
    inhoud = "".join(f"{i}\n" for i in sorted(ids)).encode()
    with open(pad, "wb") as f, gzip.GzipFile(fileobj=f, mode="wb", mtime=0) as gz:
        gz.write(inhoud)
    print(f"{pad}: {len(ids):,} ID's, {pad.stat().st_size / 1e6:.2f} MB")


def main() -> None:
    """Download de dataset, trek beide pools en schrijf ze weg."""
    with tempfile.TemporaryDirectory() as tmp:
        parquet = Path(tmp) / "monumenten.parquet"
        print(f"Downloaden van {HF_URL}")
        urllib.request.urlretrieve(HF_URL, parquet)
        tabel = pq.read_table(
            parquet,
            columns=[
                "bag_verblijfsobject_id",
                "rijksmonument",
                "gemeentelijk_monument",
            ],
        ).to_pydict()

    ids: list[str] = tabel["bag_verblijfsobject_id"]
    monumenten = [
        i
        for i, rijks, gemeentelijk in zip(
            ids, tabel["rijksmonument"], tabel["gemeentelijk_monument"]
        )
        if rijks or gemeentelijk
    ]
    print(
        f"{len(ids):,} verblijfsobjecten, waarvan {len(monumenten):,} met monumentvlag"
    )

    rng = random.Random(SEED)
    POOL_MAP.mkdir(exist_ok=True)
    schrijf_pool("willekeurig", rng.sample(ids, POOL_GROOTTE))
    schrijf_pool("monumentvlag", rng.sample(monumenten, POOL_GROOTTE))


if __name__ == "__main__":
    main()
