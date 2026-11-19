"""e-Stat「廃置分合等情報」の取り込み（fetch）と解析（build）。

    fetch: CSV を落として bronze に置く（原本は上書きしない。manifest に 1 行足す）
    build: bronze の最新 1 本から silver の Parquet 3 本を作る（検査に落ちたら何も書かない）

bronze のキー: estat/haichi/fetched=<YYYY-MM-DD>/haichi_<sha256 先頭 12>.csv
silver: $AOD_CACHE/silver/haichi/{haichi_rows,haichi_events,haichi_transitions}.parquet
"""

import csv
import hashlib
import io
import json
import os
import re
import sys
import uuid
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from urllib.parse import quote

import pyarrow as pa
import pyarrow.parquet as pq

from .. import cache, http, ledger

SOURCE = "haichi"
BASE_URL = "https://www.e-stat.go.jp/municipalities/cities/absorption-separation-of-municipalities"
PREFIX = "estat/haichi"
FIRST_YEAR = 1970

COLUMNS = [
    "事由パターン",
    "標準地域コード",
    "都道府県",
    "政令市･郡･支庁･振興局等",
    "政令市･郡･支庁･振興局等（ふりがな）",
    "市区町村",
    "市区町村（ふりがな）",
    "廃置分合等施行年月日",
    "改正事由",
    "告示年月日",
    "告示番号",
]

# 事由パターン（フォームの reason_kd の順）
PATTERNS = [
    "編入合併",
    "新設合併（コード追加あり）",
    "新設合併（コード追加なし）",
    "市制施行",
    "町制施行",
    "名称変更",
    "郡の区域変更",
    "政令指定都市施行",
    "区の新設",
    "分離",
    "分割",
    "区の再編",
    "境界変更",
    "都市種別変更",
]
REASON_KD = [1, 2, 6, 7, 16, 4, 5, 8, 9, 10, 11, 12, 13, 15]
NO_TRANSITION_PATTERNS = {"境界変更", "都市種別変更"}

ITEMS = [
    "jiyuKind",
    "htCode",
    "todoNm",
    "parentCityNm",
    "parentCityKana",
    "selfCityNm",
    "selfCityKana",
    "htCodeSDate",
    "kaiseiJiyu",
    "haitibungoKokujiDate",
    "haitibungoKokujiNo",
]

KIND_OF_PATTERN = {
    "編入合併": "absorb",
    "新設合併（コード追加あり）": "merge_new_code",
    "新設合併（コード追加なし）": "merge_reuse_code",
    "市制施行": "status_change",
    "町制施行": "status_change",
    "名称変更": "rename",
    "郡の区域変更": "recode",
    "政令指定都市施行": "designated_city",
    "区の新設": "ward_create",
    "分離": "ward_separate",
    "分割": "ward_split",
    "区の再編": "ward_reorg",
}

_CODE_OK = re.compile(r"^[0-9]{5}$")


class CheckFailed(Exception):
    """build の検査に落ちた。problems に落ちた項目を全部入れる。"""

    def __init__(self, problems: list[str]):
        super().__init__("\n".join(problems))
        self.problems = problems


class ParseError(Exception):
    pass


# --------------------------------------------------------------------------- 取得


def build_url(today: date) -> str:
    q: list[tuple[str, str]] = [
        ("year_from", str(FIRST_YEAR)),
        ("month_from", "1"),
        ("day_from", "1"),
        ("year_to", str(today.year)),
        ("month_to", str(today.month)),
        ("day_to", str(today.day)),
        ("prefecture_all", "on"),
    ]
    q += [(f"pf[{i}]", str(i)) for i in range(1, 48)]
    q += [("city_nm", "")]
    q += [(f"city_kd[{i}]", str(i)) for i in (4, 5, 6, 7)]
    q += [("reason_kind_all", "on")]
    q += [(f"reason_kd[{n}]", str(n)) for n in REASON_KD]
    q += [("border_kd", "1"), ("op", "search"), ("keyword_kd", "code")]
    q += [("item[]", name) for name in ITEMS]
    q += [("sort[]", "htCode-asc"), ("sort[]", "htCodeSDate-asc")]
    q += [("form_id", "city_merger_form"), ("source", "setup")]
    q += [("file_format", "csv"), ("charset", "UTF-8"), ("bom", "0"), ("op", "download")]
    return BASE_URL + "?" + "&".join(f"{quote(k, safe='[]')}={quote(v, safe='')}" for k, v in q)


def bronze_dir() -> Path:
    return cache.bronze_root() / PREFIX


def manifest_path() -> Path:
    return bronze_dir() / "_manifest.jsonl"


def read_manifest() -> list[dict]:
    p = manifest_path()
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def make_key(fetched: str, sha256: str) -> str:
    return f"{PREFIX}/fetched={fetched}/haichi_{sha256[:12]}.csv"


def read_csv(data: bytes) -> tuple[list[str], list[list[str]]]:
    text = data.decode("utf-8")
    recs = list(csv.reader(io.StringIO(text, newline="")))
    if not recs:
        raise ValueError("CSV が空")
    return recs[0], recs[1:]


def validate_download(content_type: str, data: bytes) -> tuple[int, str]:
    """HTML のエラーページなどを原本として置かない。(行数, sha256) を返す。"""
    if content_type.split(";")[0].strip().lower() != "application/octet-stream":
        raise ValueError(f"Content-Type が application/octet-stream でない: {content_type!r}")
    header, recs = read_csv(data)
    if header != COLUMNS:
        raise ValueError(f"ヘッダが 11 列の想定と一致しない: {header!r}")
    return len(recs), hashlib.sha256(data).hexdigest()


def store(data: bytes, fetched: str, sha256: str, url: str, rows: int) -> str:
    """原本を置いて manifest に 1 行足す。同じキーのファイルが既にあれば上書きしない。"""
    key = make_key(fetched, sha256)
    dest = cache.bronze_root() / key
    if not dest.exists():
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(dest.name + ".part")
        tmp.write_bytes(data)
        try:
            os.link(tmp, dest)  # 既にあれば FileExistsError。上書きしない
        finally:
            tmp.unlink()
    mp = manifest_path()
    mp.parent.mkdir(parents=True, exist_ok=True)
    with mp.open("a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "key": key,
                    "url": url,
                    "sha256": sha256,
                    "bytes": len(data),
                    "rows": rows,
                    "fetched_at": ledger.now_jst().isoformat(timespec="seconds"),
                },
                ensure_ascii=False,
            )
            + "\n"
        )
    return key


def fetch() -> int:
    with ledger.run(SOURCE, "fetch") as rec:
        now = ledger.now_jst()
        url = build_url(now.date())
        resp = http.get(url)
        if resp.status != 200:
            raise ValueError(f"HTTP {resp.status}")
        rows, sha = validate_download(resp.content_type, resp.body)
        rec["rows"], rec["bytes"], rec["sha256"] = rows, len(resp.body), sha
        manifest = read_manifest()
        if manifest and manifest[-1]["sha256"] == sha:
            rec["status"] = "skipped_nochange"
            print(f"変更なし（sha256 {sha[:12]}）。置かない: 直近の原本 {manifest[-1]['key']}")
            return 0
        key = store(resp.body, now.date().isoformat(), sha, url, rows)
        print(f"取得 {key}（{rows} 行, {len(resp.body)} bytes）")
    return 0


# --------------------------------------------------------------------------- 解析

# 括弧の中身は「ふりがな、コード」の形もある（溝口町(みぞぐちちょう、31404)）。浜松市の区の再編だけ全角括弧。
_CODE_RE = re.compile(r"[(（][^()（）0-9０-９]*?([0-9]{5})[)）]")
_PAREN_RE = re.compile(r"（[^）]*）|\([^)]*\)")


def _clean(seg: str, first: bool) -> str:
    """コードの直前の文字列から、つなぎ（、が を の と …し、）と注記を除いて名前だけにする。"""
    s = _PAREN_RE.sub("", seg)
    s = s.rsplit("　", 1)[-1]
    if "し、" in s:
        return s.rsplit("し、", 1)[1]
    if not first and s[:1] in ("、", "が", "を", "の", "と"):
        s = s[1:]
    return s


@dataclass(frozen=True)
class Trans:
    kind: str
    from_code: str
    from_name: str
    to_code: str
    to_name: str


@dataclass
class Ctx:
    """行全体から引く補助情報。"""

    ward_codes: set[str] = field(default_factory=set)
    city_code: dict[tuple[str, str], str] = field(default_factory=dict)  # (都道府県, 政令市名) -> コード


def make_ctx(records: list[list[str]]) -> Ctx:
    ctx = Ctx()
    for r in records:
        code, pref, parent, name = r[1], r[2], r[3], r[5]
        if parent.endswith("市"):
            if name:
                ctx.ward_codes.add(code)  # 政令市の区
            else:
                ctx.city_code[(pref, parent)] = code  # 政令市そのもの
    return ctx


def _tokenize(line: str) -> tuple[list[tuple[str, str]], str]:
    parts: list[tuple[str, str]] = []
    pos = 0
    for m in _CODE_RE.finditer(line):
        parts.append((line[pos : m.start()], m.group(1)))
        pos = m.end()
    return parts, line[pos:]


def parse_transitions(pattern: str, line: str, event_rows: list[list[str]], ctx: Ctx) -> list[Trans]:
    """改正事由の 1 行目（と、区の新設では行の情報）から、旧コード → 新コードを作る。"""
    kind = KIND_OF_PATTERN.get(pattern)
    if kind is None:
        raise ParseError(f"transitions を作らない事由パターン、または未知のパターン: {pattern}")
    parts, tail = _tokenize(line)
    raw = [s for s, _ in parts]
    ents = [(_clean(s, i == 0), c) for i, (s, c) in enumerate(parts)]
    n = len(ents)
    out: list[Trans] = []

    def pair(a: tuple[str, str], b: tuple[str, str]) -> Trans:
        return Trans(kind, a[1], a[0], b[1], b[0])

    if kind == "absorb":
        if n < 2 or not tail.startswith("に編入"):
            raise ParseError("編入の文の形でない")
        out = [pair(e, ents[-1]) for e in ents[:-1]]
    elif kind == "merge_new_code":
        # 大阪市の区どうしは「A と B が統合し、C を新設」と書かれる（合併ではなく統合）
        if n < 3 or not re.search(r"が(合併|統合)し", raw[-1]) or not tail.startswith("を新設"):
            raise ParseError("新設合併（コード追加あり）の文の形でない")
        out = [pair(e, ents[-1]) for e in ents[:-1]]
    elif kind == "merge_reuse_code":
        m = re.match(r"^が合併し、(.+?)を新設", tail)
        if n < 2 or not m:
            raise ParseError("新設合併（コード追加なし）の文の形でない")
        hits = [e for e in ents if e[0] == m.group(1)]
        if len({c for _, c in hits}) != 1:
            raise ParseError(f"新設後の名前 {m.group(1)!r} に一致する旧コードが {len({c for _, c in hits})} 個")
        out = [pair(e, hits[0]) for e in ents]
    elif kind in ("status_change", "rename"):
        if n != 2 or not re.search(r"(市制施行|町制施行|名称変更)", tail):
            raise ParseError(f"{pattern} の文の形でない")
        out = [pair(ents[0], ents[1])]
    elif kind == "recode":
        if n < 2 or n % 2 or "区域変更" not in tail:
            raise ParseError("郡の区域変更の文の形でない")
        k = n // 2
        out = [pair(ents[i], ents[i + k]) for i in range(k)]
    elif kind == "designated_city":
        if n != 2 or "政令指定都市" not in tail:
            raise ParseError("政令指定都市施行の文の形でない")
        out = [pair(ents[0], ents[1])]
    elif kind == "ward_create":
        if n == 2 and "政令指定都市" in tail:
            # 政令指定都市施行と同じ文の下に区の新設が付く行。旧市のコード → 新設された区（行のコード）
            old, new = ents
            wards = OrderedDict((r[1], r[5]) for r in event_rows if r[1] not in (old[1], new[1]))
            out = [Trans(kind, old[1], old[0], c, nm) for c, nm in wards.items()]
        elif n == 1 and tail.startswith("の新設"):
            # 区だけの新設。政令市そのもののコード → 新設された区
            r = event_rows[0]
            city = ctx.city_code.get((r[2], r[3]))
            if city is None:
                raise ParseError(f"政令市 {r[3]}（{r[2]}）のコードが行から引けない")
            out = [Trans(kind, city, r[3], ents[0][1], ents[0][0])]
        else:
            raise ParseError("区の新設の文の形でない")
    elif kind in ("ward_separate", "ward_split"):
        conn = "から分離し、" if kind == "ward_separate" else "を分割し、"
        idx = [i for i in range(1, n) if conn in raw[i]]
        if len(idx) != 1 or not tail.startswith("を新設"):
            raise ParseError(f"{pattern} の文の形でない")
        j = idx[0]
        out = [pair(a, b) for a in ents[:j] for b in ents[j:]]
    elif kind == "ward_reorg":
        if n < 3:
            raise ParseError("区の再編の文の形でない")
        pending: list[tuple[str, str]] = []
        for i in range(1, n):  # ents[0] は政令市そのもの
            seg = _PAREN_RE.sub("", raw[i]).rsplit("　", 1)[-1]
            if seg.startswith("を"):
                out += [pair(f, ents[i]) for f in pending]
                pending = []
            else:
                pending.append(ents[i])
        if pending:
            raise ParseError("区の再編の文の形でない（再編先の無い区がある）")

    # 重複を除く（順序は保つ）
    seen: set[Trans] = set()
    uniq = [t for t in out if not (t in seen or seen.add(t))]
    if not uniq:
        raise ParseError("transitions が 0 本")
    return uniq


def grain_of(t: Trans, ctx: Ctx) -> str:
    if t.kind.startswith("ward_") or t.from_code in ctx.ward_codes or t.to_code in ctx.ward_codes:
        return "ward"
    return "municipality"


def municipality_split_violations(transitions: list[dict]) -> list[str]:
    """検査 4: grain=municipality で、1 つの event の 1 つの from_code が 2 つ以上の to_code に行くもの。"""
    dest: dict[tuple[str, str], set[str]] = defaultdict(set)
    for t in transitions:
        if t["grain"] == "municipality":
            dest[(t["event_id"], t["from_code"])].add(t["to_code"])
    return [
        f"[4] event {eid} の {fc} が市町村の単位で {len(tos)} 個の to_code に行く: {sorted(tos)}"
        for (eid, fc), tos in dest.items()
        if len(tos) >= 2
    ]


# ----------------------------------------------------------------------- silver

_S = pa.string()
ROWS_SCHEMA = pa.schema([pa.field(c, _S) for c in COLUMNS] + [pa.field("row_uid", _S), pa.field("source_key", _S)])
EVENTS_SCHEMA = pa.schema(
    [
        pa.field("event_id", _S),
        pa.field("effective_date", pa.date32()),
        pa.field("pattern", _S),
        pa.field("reason_text", _S),
        pa.field("notice_date", pa.date32()),
        pa.field("notice_no", _S),
        pa.field("codes", pa.list_(_S)),
    ]
)
TRANSITIONS_SCHEMA = pa.schema(
    [
        pa.field("event_id", _S),
        pa.field("effective_date", pa.date32()),
        pa.field("pattern", _S),
        pa.field("kind", _S),
        pa.field("grain", _S),
        pa.field("from_code", _S),
        pa.field("from_name", _S),
        pa.field("to_code", _S),
        pa.field("to_name", _S),
    ]
)
SILVER_FILES = {
    "haichi_rows": ROWS_SCHEMA,
    "haichi_events": EVENTS_SCHEMA,
    "haichi_transitions": TRANSITIONS_SCHEMA,
}


def first_line(reason: str) -> str:
    return re.split(r"\r?\n", reason, maxsplit=1)[0]


def event_id_of(effective: str, pattern: str, reason: str) -> str:
    return hashlib.sha256(f"{effective}\t{pattern}\t{first_line(reason)}".encode()).hexdigest()[:16]


def _date_or_none(s: str) -> date | None:
    return date.fromisoformat(s) if s else None


def build_silver(data: bytes, source_key: str, sha12: str, prev_rows: int | None) -> dict[str, pa.Table]:
    """原本から silver の 3 表を作る。検査に落ちたら CheckFailed（落ちた項目は全部入れる）。"""
    header, recs = read_csv(data)
    problems: list[str] = []

    if header != COLUMNS:
        # 列の位置が信用できないので、これ以上は読まない
        raise CheckFailed([f"[1] ヘッダが 11 列の想定と一致しない: {header!r}"])
    short = [i for i, r in enumerate(recs, 1) if len(r) != len(COLUMNS)]
    if short:
        raise CheckFailed([f"[1] 列数が 11 でない行がある（行番号 {short[:20]}）"])

    bad_pat = sorted({r[0] for r in recs if r[0] not in PATTERNS})
    if bad_pat:
        problems.append(f"[2] 未知の事由パターン: {bad_pat}")
    bad_code = sorted({r[1] for r in recs if not _CODE_OK.match(r[1])})
    if bad_code:
        problems.append(f"[5] 標準地域コードが 5 桁の数字でない: {bad_code[:20]}")
    if prev_rows is not None and len(recs) < prev_rows:
        problems.append(f"[6] 行数 {len(recs)} が前回 build の {prev_rows} を下回る")

    ctx = make_ctx(recs)
    groups: OrderedDict[tuple[str, str, str], list[list[str]]] = OrderedDict()
    for r in recs:
        try:
            date.fromisoformat(r[7])
        except ValueError:
            problems.append(f"[形式] 施行年月日が ISO 形式でない: {r[7]!r}（コード {r[1]}）")
            continue
        groups.setdefault((r[7], r[0], first_line(r[8])), []).append(r)

    events: list[dict] = []
    transitions: list[dict] = []
    for (eff, pattern, line), erows in groups.items():
        eid = event_id_of(eff, pattern, erows[0][8])
        first = erows[0]
        codes = list(OrderedDict.fromkeys(r[1] for r in erows))
        try:
            events.append(
                {
                    "event_id": eid,
                    "effective_date": date.fromisoformat(eff),
                    "pattern": pattern,
                    "reason_text": first[8].replace("\r\n", "\n"),
                    "notice_date": _date_or_none(first[9]),
                    "notice_no": first[10] or None,
                    "codes": codes,
                }
            )
        except ValueError:
            problems.append(f"[形式] 告示年月日が ISO 形式でない: {first[9]!r}（event {eid}）")
            continue
        if pattern in NO_TRANSITION_PATTERNS or pattern not in PATTERNS:
            continue
        try:
            ts = parse_transitions(pattern, line, erows, ctx)
        except ParseError as e:
            problems.append(f"[3] transitions を作れない event: {eff} {pattern} {line!r} — {e}")
            continue
        for t in ts:
            transitions.append(
                {
                    "event_id": eid,
                    "effective_date": date.fromisoformat(eff),
                    "pattern": pattern,
                    "kind": t.kind,
                    "grain": grain_of(t, ctx),
                    "from_code": t.from_code,
                    "from_name": t.from_name,
                    "to_code": t.to_code,
                    "to_name": t.to_name,
                }
            )

    problems += municipality_split_violations(transitions)
    bad_t = sorted({c for t in transitions for c in (t["from_code"], t["to_code"]) if not _CODE_OK.match(c)})
    if bad_t:
        problems.append(f"[5] transitions のコードが 5 桁の数字でない: {bad_t[:20]}")
    if problems:
        raise CheckFailed(problems)

    events.sort(key=lambda e: (e["effective_date"], e["event_id"]))
    transitions.sort(key=lambda t: (t["effective_date"], t["event_id"]))  # 安定ソート。event 内の順は保つ
    rows_out = [
        {**dict(zip(COLUMNS, r)), "row_uid": f"{sha12}#{i}", "source_key": source_key} for i, r in enumerate(recs, 1)
    ]
    return {
        "haichi_rows": pa.Table.from_pylist(rows_out, schema=ROWS_SCHEMA),
        "haichi_events": pa.Table.from_pylist(events, schema=EVENTS_SCHEMA),
        "haichi_transitions": pa.Table.from_pylist(transitions, schema=TRANSITIONS_SCHEMA),
    }


def silver_dir() -> Path:
    return cache.silver_root() / SOURCE


def write_silver(tables: dict[str, pa.Table]) -> None:
    """3 本とも一時ファイルに書いてから置き換える（途中で失敗しても既存のものを壊さない）。"""
    d = silver_dir()
    d.mkdir(parents=True, exist_ok=True)
    tag = uuid.uuid4().hex[:8]
    tmps = {}
    try:
        for name, table in tables.items():
            tmp = d / f".{name}.{tag}.tmp"
            pq.write_table(table, tmp)
            tmps[name] = tmp
        for name, tmp in tmps.items():
            os.replace(tmp, d / f"{name}.parquet")
    finally:
        for tmp in tmps.values():
            tmp.unlink(missing_ok=True)


def build() -> int:
    try:
        with ledger.run(SOURCE, "build") as rec:
            manifest = read_manifest()
            if not manifest:
                raise FileNotFoundError("bronze に原本が無い。先に fetch")
            m = manifest[-1]
            data = (cache.bronze_root() / m["key"]).read_bytes()
            sha = hashlib.sha256(data).hexdigest()
            rec["bytes"], rec["sha256"] = len(data), sha
            if sha != m["sha256"]:
                raise ValueError(f"原本の sha256 が manifest と違う: {m['key']}")
            prev = ledger.last_ok_rows(SOURCE, "build")
            header, recs = read_csv(data)
            rec["rows"] = len(recs)
            tables = build_silver(data, m["key"], sha[:12], prev)
            write_silver(tables)
            print(f"build 完了 {m['key']}: " + ", ".join(f"{k} {t.num_rows} 行" for k, t in tables.items()))
    except CheckFailed as e:
        print("検査に落ちた（silver は書いていない）:", file=sys.stderr)
        for p in e.problems:
            print(f"  - {p}", file=sys.stderr)
        return 1
    return 0
