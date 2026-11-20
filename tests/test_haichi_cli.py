import hashlib
import json
from datetime import date

import pyarrow.parquet as pq
import pytest

from aod import cache, http, ledger
from aod.__main__ import main
from aod.sources import haichi as h

from .conftest import to_bytes


def _ledger():
    return [json.loads(line) for line in cache.ledger_path().read_text(encoding="utf-8").splitlines()]


@pytest.fixture
def fake_get(monkeypatch, sample_bytes):
    state = {"body": sample_bytes, "ctype": "application/octet-stream", "urls": []}

    def get(url):
        state["urls"].append(url)
        return http.Response(200, state["ctype"], state["body"])

    monkeypatch.setattr(http, "get", get)
    return state


def test_build_url_matches_the_frozen_query():
    url = h.build_url(date(2026, 11, 19))
    assert url.startswith(h.BASE_URL + "?year_from=1970&month_from=1&day_from=1&year_to=2026&month_to=11&day_to=19&prefecture_all=on&pf[1]=1&pf[2]=2")
    assert "&pf[47]=47&city_nm=&city_kd[4]=4&city_kd[5]=5&city_kd[6]=6&city_kd[7]=7&reason_kind_all=on&reason_kd[1]=1&reason_kd[2]=2&reason_kd[6]=6&reason_kd[7]=7&reason_kd[16]=16&reason_kd[4]=4" in url
    assert "&reason_kd[15]=15&border_kd=1&op=search&keyword_kd=code&item[]=jiyuKind&item[]=htCode" in url
    assert "&item[]=haitibungoKokujiNo&sort[]=htCode-asc&sort[]=htCodeSDate-asc&form_id=city_merger_form&source=setup" in url
    assert url.endswith("&file_format=csv&charset=UTF-8&bom=0&op=download")


def test_fetch_stores_then_skips_nochange(fake_get, sample_bytes):
    assert main(["haichi", "fetch"]) == 0
    sha = hashlib.sha256(sample_bytes).hexdigest()
    m = h.read_manifest()
    assert len(m) == 1 and m[0]["sha256"] == sha and m[0]["bytes"] == len(sample_bytes)
    assert m[0]["key"].endswith(f"/haichi_{sha[:12]}.csv") and "/fetched=" in m[0]["key"]
    assert (cache.bronze_root() / m[0]["key"]).read_bytes() == sample_bytes

    assert main(["haichi", "fetch"]) == 0
    assert len(h.read_manifest()) == 1
    runs = _ledger()
    assert [r["status"] for r in runs] == ["ok", "skipped_nochange"]
    assert runs[0]["rows"] == m[0]["rows"] and runs[0]["sha256"] == sha


def test_fetch_refuses_html(fake_get):
    fake_get["ctype"] = "text/html; charset=UTF-8"
    fake_get["body"] = b"<html>error</html>"
    with pytest.raises(ValueError):
        main(["haichi", "fetch"])
    assert h.read_manifest() == []
    assert not (cache.bronze_root() / "estat").exists()
    run = _ledger()[-1]
    assert run["status"] == "fail" and run["error_class"] == "ValueError"


def test_fetch_refuses_wrong_header(fake_get, sample_bytes):
    header, recs = h.read_csv(sample_bytes)
    fake_get["body"] = to_bytes(header[:-1], [r[:-1] for r in recs])
    with pytest.raises(ValueError):
        main(["haichi", "fetch"])
    assert h.read_manifest() == []


def test_build_writes_three_parquet_and_ledger(fake_get):
    main(["haichi", "fetch"])
    assert main(["haichi", "build"]) == 0
    d = h.silver_dir()
    for name in ("haichi_rows", "haichi_events", "haichi_transitions"):
        assert pq.read_table(d / f"{name}.parquet").num_rows > 0
    run = _ledger()[-1]
    assert (run["source"], run["step"], run["status"]) == ("haichi", "build", "ok")
    assert set(run) >= {
        "run_id", "started_at", "finished_at", "duration_s", "rows", "bytes", "sha256",
        "error_class", "error_message", "code_version",
    }  # fmt: skip
    assert run["started_at"].endswith("+09:00")


def test_build_failure_writes_nothing_and_returns_1(fake_get, sample_bytes, capsys):
    main(["haichi", "fetch"])
    assert main(["haichi", "build"]) == 0
    before = (h.silver_dir() / "haichi_events.parquet").read_bytes()

    # 行数が減った原本を新しく置く → 検査 6 で落ちる。既存の silver は壊さない
    header, recs = h.read_csv(sample_bytes)
    smaller = to_bytes(header, recs[:-3])
    fake_get["body"] = smaller
    assert main(["haichi", "fetch"]) == 0
    capsys.readouterr()
    assert main(["haichi", "build"]) == 1
    assert "[6]" in capsys.readouterr().err
    assert (h.silver_dir() / "haichi_events.parquet").read_bytes() == before
    assert not list(h.silver_dir().glob(".*.tmp"))
    run = _ledger()[-1]
    assert run["status"] == "fail" and run["error_class"] == "CheckFailed"


def test_build_without_bronze_fails_with_ledger_row():
    with pytest.raises(FileNotFoundError):
        main(["haichi", "build"])
    assert _ledger()[-1]["status"] == "fail"


def test_usage():
    assert main(["haichi"]) == 2
    assert main(["nope", "fetch"]) == 2


def test_code_version_is_a_string():
    assert isinstance(ledger.code_version(), str)


def test_fetch_refuses_header_only_csv(fake_get, sample_bytes):
    fake_get["body"] = sample_bytes.split(b"\n", 1)[0] + b"\n"
    with pytest.raises(ValueError, match="0 行"):
        h.fetch()
    assert not h.manifest_path().exists()
    assert _ledger()[-1]["status"] == "fail"
