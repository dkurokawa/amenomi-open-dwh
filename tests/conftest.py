import csv
import io
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_bytes() -> bytes:
    return (FIXTURES / "haichi_sample.csv").read_bytes()


def to_bytes(header: list[str], recs: list[list[str]]) -> bytes:
    buf = io.StringIO(newline="")
    w = csv.writer(buf, quoting=csv.QUOTE_ALL, lineterminator="\n")
    w.writerow(header)
    w.writerows(recs)
    return buf.getvalue().encode("utf-8")


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    """どのテストも実際のキャッシュに触れない。"""
    monkeypatch.setenv("AOD_CACHE", str(tmp_path / "cache"))
