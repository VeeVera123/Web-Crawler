"""Shared run logging: one tidy console format, quiet third-party loggers, and an ERROR DIGEST.

Every entry point calls setup() once. From then on:

  * INFO lines print as they happen, in one compact format.
  * WARNING / ERROR records are NOT printed one by one. They are counted per source (an ATS platform, "supabase",
    "classifier", ...) and flush() prints ONE block listing only the sources that had problems: how many, the most common
    error code, and one example. A run with no problems prints no error block at all.
  * The digest also prints automatically when the process exits (normal end or crash), so nothing is lost.

A message that must be shown immediately can pass extra={"keep": True}. Code that already aggregates failures itself (the
scrape-failure collector in ats_scrapers.py) reports through add().
"""
from __future__ import annotations

import atexit
import logging
import re
import sys
import threading
from collections import Counter

_FORMAT = "%(asctime)s %(levelname)-5s %(message)s"
_DATEFMT = "%H:%M:%S"
_QUIET = ("httpx", "httpcore", "urllib3", "asyncio", "aiohttp", "aiohttp.access", "openai", "hpack", "charset_normalizer",
          "filelock", "fsspec", "duckdb", "botocore", "PIL", "matplotlib", "websockets", "h2", "anyio")

_lock = threading.Lock()
_buckets: dict[tuple[str, str], dict] = {}   # (source, "ERROR"/"WARN") -> {"n", "example", "codes"}
_installed = False

_SOURCE_RES = (
    re.compile(r"^\s*\[([\w .-]{2,30})\]"),                 # "[workday] ..."
    re.compile(r"\bfailed for ([\w.-]{2,30})\b"),          # "Supabase upsert failed for joincom: ..."
    re.compile(r"^\s*([A-Za-z][\w.-]{1,24}):\s"),          # "Taleo: could not ..."
)
_CODE_RES = (
    re.compile(r"\bHTTP[ /]?(\d{3})\b"),
    re.compile(r"\b(\d{3}) (?:Client|Server) Error\b"),
    re.compile(r'"code"\s*:\s*"?(\w{3,8})"?'),
    re.compile(r"\b([A-Z][A-Za-z]+(?:Error|Exception|Timeout))\b"),
    re.compile(r"\bstatus[ =:]+(\d{3})\b", re.I),
)
_URL_QUERY_RE = re.compile(r"\?[^\s'\"]*")


def _source_of(record: logging.LogRecord, message: str) -> str:
    if message.lstrip().lower().startswith("supabase"):
        return "supabase"
    for rx in _SOURCE_RES:
        m = rx.search(message)
        if m:
            return m.group(1).strip().lower()
    name = record.name.split(".")[-1]
    return {"supabase_handler": "supabase", "__main__": "run"}.get(name, name)


def _code_of(record: logging.LogRecord, message: str) -> str:
    for rx in _CODE_RES:
        m = rx.search(message)
        if m:
            code = m.group(1)
            return f"HTTP {code}" if code.isdigit() and len(code) == 3 else code
    if record.exc_info and record.exc_info[0]:
        return record.exc_info[0].__name__
    return "-"


def _tidy(message: str, limit: int = 150) -> str:
    one = " ".join(message.split())
    one = _URL_QUERY_RE.sub("?…", one)
    return one if len(one) <= limit else one[: limit - 1] + "…"


def add(source: str, n: int = 1, example: str = "", code: str = "-", level: str = "ERROR") -> None:
    """Record n problems for `source` (used by code that aggregates its own failures)."""
    with _lock:
        b = _buckets.setdefault((source.lower(), level), {"n": 0, "example": "", "codes": Counter()})
        b["n"] += n
        b["codes"][code] += n
        if not b["example"]:
            b["example"] = _tidy(example)


class _Digest(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno < logging.WARNING or getattr(record, "keep", False):
            return True
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        add(_source_of(record, message), 1, message, _code_of(record, message),
            "ERROR" if record.levelno >= logging.ERROR else "WARN")
        return False


def flush(title: str = "Errors & warnings") -> None:
    """Print the digest (only sources that had problems) and clear it. Prints nothing when the run was clean."""
    with _lock:
        items = sorted(_buckets.items(), key=lambda kv: -kv[1]["n"])
        _buckets.clear()
    if not items:
        return
    width = max(len(src) for (src, _), _b in items)
    lines = [f"── {title}: {sum(b['n'] for _, b in items)} across {len({s for (s, _), _b in items})} source(s) " + "─" * 12]
    for (src, level), b in items:
        top = b["codes"].most_common(1)[0][0]
        lines.append(f"  {src:<{width}}  {b['n']:>5} {level:<5} {top:<14} e.g. {b['example']}")
    logging.getLogger("run_log").info("\n".join(lines), extra={"keep": True})


def setup(level: int = logging.INFO) -> None:
    """Install the console handler, the digest and the exit hook. Safe to call more than once."""
    global _installed
    root = logging.getLogger()
    root.setLevel(level)
    if not _installed:
        for h in list(root.handlers):
            root.removeHandler(h)
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_FORMAT, _DATEFMT))
        handler.addFilter(_Digest())
        root.addHandler(handler)
        for name in _QUIET:
            logging.getLogger(name).setLevel(logging.ERROR)
        atexit.register(flush)
        _installed = True


# ── Progress ────────────────────────────────────────────────────────────────

def _fmt_duration(seconds: float) -> str:
    seconds = max(int(seconds), 0)
    if seconds < 90:
        return f"{seconds}s"
    if seconds < 5400:
        return f"{round(seconds / 60)}m"
    return f"{seconds // 3600}h {seconds % 3600 // 60:02d}m"


class Progress:
    """One status line at most every `every_s` seconds: how far along, the speed, and the time left at that speed.

        prog = run_log.Progress("pages", total=31808)
        ... prog.update(done, postings=11777)      # call as often as you like; it only prints when due
        prog.finish(done, postings=11777)          # final line

    Prints like:  ▸ pages 26,700/31,808 · 83.9% · 27.2/s · ~3m left · postings 11,777
    """

    def __init__(self, label: str, total: int | None = None, every_s: float = 120.0, unit: str = "") -> None:
        import time as _time
        self._time = _time
        self.label, self.total, self.every_s, self.unit = label, total, every_s, unit
        self.start = _time.monotonic()
        self._last = self.start
        self._lock = threading.Lock()
        self._log = logging.getLogger("progress")

    def _line(self, done: int, stats: dict, final: bool) -> str:
        elapsed = max(self._time.monotonic() - self.start, 0.001)
        rate = done / elapsed
        parts = [f"▸ {self.label} {done:,}" + (f"/{self.total:,}" if self.total else "")]
        if self.total:
            parts.append(f"{done / self.total * 100:.1f}%")
        parts.append(f"{rate:.1f}/s")
        if final:
            parts.append(f"done in {_fmt_duration(elapsed)}")
        elif self.total and done and done < self.total:
            parts.append(f"~{_fmt_duration((self.total - done) / max(rate, 1e-9))} left")
        parts.extend(f"{k.replace('_', ' ')} {v:,}" if isinstance(v, int) else f"{k.replace('_', ' ')} {v}" for k, v in stats.items())
        return " · ".join(parts)

    def update(self, done: int, **stats) -> None:
        now = self._time.monotonic()
        with self._lock:
            if now - self._last < self.every_s:
                return
            self._last = now
        self._log.info(self._line(done, stats, False), extra={"keep": True})

    def finish(self, done: int, **stats) -> None:
        self._log.info(self._line(done, stats, True), extra={"keep": True})
