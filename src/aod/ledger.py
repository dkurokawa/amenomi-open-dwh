"""実行台帳（`$AOD_CACHE/_ledger/runs.jsonl`）。1 行 = 1 実行。例外でも必ず 1 行書く。"""

import json
import subprocess
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import cache

JST = timezone(timedelta(hours=9))


def now_jst() -> datetime:
    return datetime.now(JST)


def code_version() -> str:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).resolve().parent,
            capture_output=True,
            text=True,
            timeout=10,
        )
        out = r.stdout.strip()
        return out if r.returncode == 0 and out else "unknown"
    except Exception:
        return "unknown"


def read_runs() -> list[dict]:
    p = cache.ledger_path()
    if not p.exists():
        return []
    return [json.loads(line) for line in p.read_text(encoding="utf-8").splitlines() if line.strip()]


def last_ok_rows(source: str, step: str) -> int | None:
    """直近に成功した実行の rows。無ければ None。"""
    for r in reversed(read_runs()):
        if r.get("source") == source and r.get("step") == step and r.get("status") == "ok" and r.get("rows") is not None:
            return r["rows"]
    return None


@contextmanager
def run(source: str, step: str):
    """with ledger.run("haichi", "fetch") as rec: ... rec["rows"] = ...

    本体が `rec["status"] = "skipped_nochange"` を入れなければ ok。例外なら fail にして再送出する。
    """
    started = now_jst()
    t0 = time.monotonic()
    rec: dict = {
        "run_id": str(uuid.uuid4()),
        "source": source,
        "step": step,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": None,
        "duration_s": None,
        "status": "ok",
        "rows": None,
        "bytes": None,
        "sha256": None,
        "error_class": None,
        "error_message": None,
        "code_version": code_version(),
    }
    try:
        yield rec
    except BaseException as e:
        rec["status"] = "fail"
        rec["error_class"] = type(e).__name__
        rec["error_message"] = str(e)
        raise
    finally:
        rec["finished_at"] = now_jst().isoformat(timespec="seconds")
        rec["duration_s"] = round(time.monotonic() - t0, 3)
        p = cache.ledger_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
