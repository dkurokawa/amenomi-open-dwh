import hashlib
import json
from collections import Counter

import pytest

from aod.sources import haichi as h

from .conftest import FIXTURES, to_bytes


def _build(data: bytes, prev_rows=None):
    return h.build_silver(data, "estat/haichi/fetched=2026-11-19/haichi_aaaaaaaaaaaa.csv", "aaaaaaaaaaaa", prev_rows)


def _tuples(ts):
    return [(t.kind, t.from_code, t.from_name, t.to_code, t.to_name) for t in ts]


def test_fixture_builds_and_covers_all_14_patterns(sample_bytes):
    tables = _build(sample_bytes)
    events = tables["haichi_events"].to_pylist()
    assert {e["pattern"] for e in events} == set(h.PATTERNS)
    assert len(h.PATTERNS) == 14
    assert tables["haichi_rows"].num_rows == len(h.read_csv(sample_bytes)[1])


def test_expected_transitions_per_event(sample_bytes):
    """fixture の event ごとの期待値（実物で突き合わせて固定した値）と一致する。"""
    expected = json.loads((FIXTURES / "haichi_sample_expected.json").read_text(encoding="utf-8"))
    tables = _build(sample_bytes)
    events = {e["event_id"]: e for e in tables["haichi_events"].to_pylist()}
    got: dict[tuple, list] = {}
    for t in tables["haichi_transitions"].to_pylist():
        e = events[t["event_id"]]
        key = (str(e["effective_date"]), e["pattern"], e["reason_text"].split("\n")[0])
        got.setdefault(key, []).append([t["kind"], t["grain"], t["from_code"], t["from_name"], t["to_code"], t["to_name"]])
    assert len(expected) == len(got)
    for exp in expected:
        key = (exp["effective_date"], exp["pattern"], exp["reason_first_line"])
        assert got[key] == exp["transitions"], key


def test_boundary_and_city_type_events_have_no_transitions(sample_bytes):
    tables = _build(sample_bytes)
    events = tables["haichi_events"].to_pylist()
    with_t = {t["event_id"] for t in tables["haichi_transitions"].to_pylist()}
    for e in events:
        assert (e["event_id"] in with_t) == (e["pattern"] not in h.NO_TRANSITION_PATTERNS)


def test_shiga_2004_2010_all_present(sample_bytes):
    tables = _build(sample_bytes)
    trs = tables["haichi_transitions"].to_pylist()
    shiga = [t for t in trs if t["from_code"].startswith("25") and 2004 <= t["effective_date"].year <= 2010]
    # 甲賀・野洲・湖南・高島・東近江・米原・長浜・近江八幡・愛荘・大津 など
    assert {"25209", "25210", "25211", "25212", "25213", "25214", "25203", "25204", "25425", "25201"} <= {
        t["to_code"] for t in shiga
    }


def test_multiline_reason_and_event_id(sample_bytes):
    tables = _build(sample_bytes)
    events = tables["haichi_events"].to_pylist()
    multi = [e for e in events if "\n" in e["reason_text"]]
    assert multi and all("\r" not in e["reason_text"] for e in events)
    e = next(e for e in events if e["reason_text"].startswith("川崎市") is False and "三豊市(37208)" in e["reason_text"])
    first = e["reason_text"].split("\n")[0]
    assert e["event_id"] == hashlib.sha256(f"{e['effective_date']}\t{e['pattern']}\t{first}".encode()).hexdigest()[:16]
    assert e["codes"][0] == "37208" or "37421" in e["codes"]


def test_rows_table_keeps_original_and_adds_uid(sample_bytes):
    rows = _build(sample_bytes)["haichi_rows"].to_pylist()
    assert list(rows[0])[:11] == h.COLUMNS
    assert rows[0]["row_uid"] == "aaaaaaaaaaaa#1" and rows[1]["row_uid"] == "aaaaaaaaaaaa#2"
    assert {r["source_key"] for r in rows} == {"estat/haichi/fetched=2026-11-19/haichi_aaaaaaaaaaaa.csv"}
    # 改行はそのまま残す
    assert any("\r\n" in r["改正事由"] for r in rows)


def test_fullwidth_paren_hamamatsu(sample_bytes):
    tables = _build(sample_bytes)
    trs = [t for t in tables["haichi_transitions"].to_pylist() if t["kind"] == "ward_reorg"]
    pairs = {(t["from_code"], t["to_code"]) for t in trs}
    assert pairs == {
        ("22131", "22138"), ("22132", "22138"), ("22133", "22138"), ("22134", "22138"), ("22135", "22138"),
        ("22135", "22139"), ("22136", "22139"), ("22137", "22140"),
    }  # fmt: skip
    assert Counter(t["grain"] for t in trs) == {"ward": len(trs)}


def test_merge_reuse_code_keeps_self_row(sample_bytes):
    trs = _build(sample_bytes)["haichi_transitions"].to_pylist()
    kushiro = [t for t in trs if t["kind"] == "merge_reuse_code" and t["to_code"] == "01206"]
    assert {(t["from_code"], t["to_code"]) for t in kushiro} == {("01206", "01206"), ("01666", "01206"), ("01669", "01206")}


def test_status_change_same_code_and_furigana_paren(sample_bytes):
    trs = _build(sample_bytes)["haichi_transitions"].to_pylist()
    assert any(t["from_code"] == t["to_code"] == "13305" and t["kind"] == "status_change" for t in trs)
    assert any(t["from_code"] == t["to_code"] == "31404" and t["kind"] == "rename" for t in trs)


def test_designated_city_municipality_and_ward_create(sample_bytes):
    trs = _build(sample_bytes)["haichi_transitions"].to_pylist()
    dc = [t for t in trs if t["kind"] == "designated_city"]
    assert dc and all(t["grain"] == "municipality" for t in dc)
    wc = [t for t in trs if t["kind"] == "ward_create"]
    assert wc and all(t["grain"] == "ward" for t in wc)


# ---------------------------------------------------------------- 検査


def test_check4_flags_ward_split_placed_in_municipality_grain():
    """区の分割を市町村の grain に誤って入れると落ちる。"""
    ctx = h.Ctx(ward_codes={"40102", "40106", "40107"})
    ts = h.parse_transitions("分割", "小倉区(40102)を分割し、小倉北区(40106)、小倉南区(40107)を新設", [], ctx)
    rows = [
        {"event_id": "e1", "from_code": t.from_code, "to_code": t.to_code, "grain": "municipality"} for t in ts
    ]
    msgs = h.municipality_split_violations(rows)
    assert len(msgs) == 1 and "40102" in msgs[0] and msgs[0].startswith("[4]")
    # ward の grain なら通る
    assert h.municipality_split_violations([{**r, "grain": "ward"} for r in rows]) == []


def test_check4_passes_when_split_is_across_different_events():
    rows = [
        {"event_id": "e1", "from_code": "1", "to_code": "2", "grain": "municipality"},
        {"event_id": "e2", "from_code": "1", "to_code": "3", "grain": "municipality"},
    ]
    assert h.municipality_split_violations(rows) == []


def _sample_recs(sample_bytes):
    return h.read_csv(sample_bytes)


def test_header_mismatch_stops(sample_bytes):
    header, recs = _sample_recs(sample_bytes)
    with pytest.raises(h.CheckFailed) as e:
        _build(to_bytes(header[:-1], [r[:-1] for r in recs]))
    assert e.value.problems[0].startswith("[1]")


def test_all_failing_checks_are_reported_together(sample_bytes):
    header, recs = _sample_recs(sample_bytes)
    bad = [list(r) for r in recs]
    bad[0][0] = "未知のパターン"  # 検査 2
    bad[1][1] = "1234"  # 検査 5
    # 検査 3: 編入合併の文を壊す
    target = next(r for r in bad if r[0] == "編入合併")
    for r in bad:
        if r[0] == "編入合併" and r[8] == target[8] and r[7] == target[7]:
            r[8] = "これは文の形をしていない"
    with pytest.raises(h.CheckFailed) as e:
        _build(to_bytes(header, bad), prev_rows=len(bad) + 1)  # 検査 6
    tags = {p[:3] for p in e.value.problems}
    assert {"[2]", "[3]", "[5]", "[6]"} <= tags


def test_row_count_must_not_drop(sample_bytes):
    n = len(_sample_recs(sample_bytes)[1])
    _build(sample_bytes, prev_rows=n)
    with pytest.raises(h.CheckFailed) as e:
        _build(sample_bytes, prev_rows=n + 1)
    assert [p[:3] for p in e.value.problems] == ["[6]"]


def test_unparseable_event_is_listed_with_its_text(sample_bytes):
    header, recs = _sample_recs(sample_bytes)
    bad = [list(r) for r in recs]
    for r in bad:
        if r[0] == "名称変更":
            r[8] = "広島町がどこかに名称変更"
    with pytest.raises(h.CheckFailed) as e:
        _build(to_bytes(header, bad))
    assert any("広島町がどこかに名称変更" in p and p.startswith("[3]") for p in e.value.problems)


def test_merge_reuse_code_name_must_match(sample_bytes):
    header, recs = _sample_recs(sample_bytes)
    bad = [list(r) for r in recs]
    for r in bad:
        if "釧路市を新設" in r[8]:
            r[8] = r[8].replace("釧路市を新設", "くしろ市を新設")
    with pytest.raises(h.CheckFailed) as e:
        _build(to_bytes(header, bad))
    assert any("くしろ市" in p for p in e.value.problems)
