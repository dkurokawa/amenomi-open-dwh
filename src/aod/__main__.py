"""python -m aod <source> <step>"""

import sys

from .sources import haichi

SOURCES = {"haichi": haichi}


def main(argv: list[str]) -> int:
    if len(argv) != 2 or argv[0] not in SOURCES or argv[1] not in ("fetch", "build"):
        print("usage: python -m aod haichi {fetch,build}", file=sys.stderr)
        return 2
    mod = SOURCES[argv[0]]
    return getattr(mod, argv[1])()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
