"""ローカルのキャッシュの置き場。`AOD_CACHE` で変えられる（既定 ~/.cache/amenomi-open-dwh）。

    bronze/   取得した原本（上書きしない）
    silver/   原本から作った Parquet
    _ledger/  実行台帳
"""

import os
from pathlib import Path


def cache_dir() -> Path:
    return Path(os.environ.get("AOD_CACHE") or "~/.cache/amenomi-open-dwh").expanduser()


def bronze_root() -> Path:
    return cache_dir() / "bronze"


def silver_root() -> Path:
    return cache_dir() / "silver"


def ledger_path() -> Path:
    return cache_dir() / "_ledger" / "runs.jsonl"
