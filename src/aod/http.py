"""薄い HTTP 取得。User-Agent 固定・リクエストの間を 1 秒あける・timeout 180 秒。"""

import time
import urllib.request
from dataclasses import dataclass

USER_AGENT = "amenomi-open-dwh/0.1 (+https://github.com/dkurokawa/amenomi-open-dwh; research use of e-Stat open data)"
REQUEST_INTERVAL_SEC = 1.0
TIMEOUT_SEC = 180

_last_request = 0.0


@dataclass(frozen=True)
class Response:
    status: int
    content_type: str
    body: bytes


def get(url: str) -> Response:
    global _last_request
    wait = REQUEST_INTERVAL_SEC - (time.monotonic() - _last_request)
    if wait > 0:
        time.sleep(wait)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SEC) as r:
            return Response(r.status, r.headers.get("Content-Type", ""), r.read())
    finally:
        _last_request = time.monotonic()
