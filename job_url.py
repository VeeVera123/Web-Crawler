"""Job-URL identity: when are two job_urls the same posting?

`jobs.job_url` is the table's unique key, and every "have we seen this job?"
check used to compare raw URL strings. That treats the same posting reached
through two spellings as two jobs, which shows up as the same role twice in
Notion. The spellings seen in production:

  * board-slug case   jobs.ashbyhq.com/ashby/<id>   vs  .../Ashby/<id>
                      <co>.wd5.myworkdayjobs.com/acucareers/... vs /ACUCareers/...
    (the discovery registry holds BOTH spellings of a board, two different
    shards scrape them in the same run, and both inserts succeed because the
    strings differ)
  * trailing slash    empcloud.com/project-management/  vs  .../project-management
  * tracking params   linkedin.com/jobs/view/<slug>-<id>?position=2&refId=..&trackingId=..
                      (refId/trackingId change on every crawl, so the same job
                      is "new" every time)

Two forms are defined here:

  canonical_job_url(url)  what a NEW row stores. Deterministic, so two shards
                          racing on the same posting under different slug
                          case write the SAME string and the existing
                          on_conflict=job_url upsert merges them. Conservative:
                          never changes anything that could stop the link
                          opening.
  url_key(url)            what every "already known?" check compares. Looser
                          (ignores path case and trailing slashes), so legacy
                          rows stored under another spelling are still matched.
                          Twins that still slip through (e.g. a trailing-slash
                          pair inserted concurrently) are removed by
                          supabase_handler.mark_duplicate_jobs_vetoed().

Identity-bearing parts are never dropped: gh_jid / jobId / job / id / openingID
query parameters and #fragment anchors (single-page career sites use "#50")
survive in both forms. Query VALUES keep their case (some are base64), only
path case is folded; job ids are random/numeric so path-case collisions between
two genuinely different postings do not occur (checked against every row in
`jobs`: the only case-only twins are the same posting).

Pure functions, no I/O, no imports from the rest of the project.
"""

from __future__ import annotations

import re
from typing import Iterable, Iterator
from urllib.parse import urlsplit, urlunsplit

# Query parameters that describe HOW a visitor arrived, never WHICH job.
_TRACKING_PARAMS = frozenset({
    "refid", "trackingid", "position", "pagenum", "currentjobid", "trk", "trkinfo",
    "gclid", "fbclid", "msclkid", "dclid", "yclid", "igshid", "gbraid", "wbraid",
    "src", "source", "gh_src", "ref", "referrer", "referral", "referred_by",
    "ebp", "iis", "iisn", "sid", "campaign", "fromsearch", "mkt_tok",
})
_TRACKING_PREFIXES = ("utm_", "lever-", "mc_", "_hs", "pk_", "hsa_")

_PCT_RE = re.compile(r"%[0-9a-fA-F]{2}")
_LOCALE_RE = re.compile(r"^[a-z]{2}(?:-[A-Za-z]{2})?$")


def _is_tracking(name: str) -> bool:
    n = name.lower()
    return n in _TRACKING_PARAMS or n.startswith(_TRACKING_PREFIXES)


def _upper_pct(s: str) -> str:
    """%2f and %2F are the same byte; normalise the hex digits."""
    return _PCT_RE.sub(lambda m: m.group(0).upper(), s)


def _lower_pct_safe(s: str) -> str:
    """Lowercase everything except the hex digits of %XX escapes."""
    return _upper_pct(s.lower())


def _board_slug_index(host: str, segs: list[str]) -> int | None:
    """Index in `segs` of the board slug for ATSs whose board slug is
    case-insensitive (both spellings serve the same board), else None.
    Extend here only after confirming the ATS really ignores slug case."""
    if host == "jobs.ashbyhq.com":
        return 0 if segs else None
    if host.endswith(".myworkdayjobs.com"):
        if segs and _LOCALE_RE.match(segs[0]) and len(segs) > 1:
            return 1  # /en-US/<site>/job/...
        return 0 if segs else None
    return None


def _split(url: str):
    sp = urlsplit(url.strip())
    return sp.scheme.lower(), sp.netloc.lower(), sp.path, sp.query, sp.fragment


def _clean_query(query: str) -> list[str]:
    """Raw `a=b` pieces without tracking params; no decoding/re-encoding so
    the rest of the query string stays byte-for-byte what the ATS emitted."""
    kept = []
    for piece in query.split("&"):
        if not piece:
            continue
        name = piece.split("=", 1)[0]
        if _is_tracking(name):
            continue
        kept.append(piece)
    return kept


def canonical_job_url(url: str) -> str:
    """The spelling a newly inserted row stores (see module docstring)."""
    if not url or "://" not in url:
        return url
    # Deliberately conservative: only changes that cannot affect whether the
    # link opens (host case, tracking params, and the case-insensitive board
    # slug of Ashby/Workday). Everything else in the
    # path, including a trailing slash, stays as the site emitted it (some
    # servers only serve one form); url_key() and the duplicate sweep handle
    # slash/escape-case twins instead.
    scheme, host, path, query, frag = _split(url)
    segs = [s for s in path.split("/") if s]
    if host == "jobs.ashbyhq.com":
        # slug and posting id are both case-insensitive (ids are lowercase uuids)
        path = "/" + "/".join(_lower_pct_safe(s) for s in segs)
    else:
        idx = _board_slug_index(host, segs)
        if idx is not None and idx < len(segs):
            segs[idx] = _lower_pct_safe(segs[idx])
            path = "/" + "/".join(segs)
    return urlunsplit((scheme, host, path, "&".join(_clean_query(query)), frag))


def url_key(url: str) -> str:
    """Identity used to decide "is this job already stored?"."""
    if not url or "://" not in url:
        return url or ""
    scheme, host, path, query, frag = _split(url)
    path = _lower_pct_safe(re.sub(r"/{2,}", "/", path))
    if len(path) > 1:
        path = path.rstrip("/") or "/"
    pieces = sorted(
        (p.split("=", 1)[0].lower() + ("=" + p.split("=", 1)[1] if "=" in p else ""))
        for p in _clean_query(query))
    return urlunsplit((scheme, host, path, "&".join(pieces), frag))


class UrlSet:
    """Set of job URLs compared by identity (`url_key`), remembering the
    spelling each one was first added under. `stored(url)` returns that
    spelling: touching an already-known job must write the STORED url, or the
    on_conflict=job_url upsert would not match and would insert a twin."""

    def __init__(self, urls: Iterable[str] = ()):
        self._by_key: dict[str, str] = {}
        self.update(urls)

    def add(self, url: str) -> None:
        if url:
            self._by_key.setdefault(url_key(url), url)

    def update(self, urls: Iterable[str]) -> None:
        for u in urls:
            self.add(u)

    def stored(self, url: str) -> str | None:
        return self._by_key.get(url_key(url)) if url else None

    def __contains__(self, url: object) -> bool:
        return isinstance(url, str) and bool(url) and url_key(url) in self._by_key

    def __len__(self) -> int:
        return len(self._by_key)

    def __iter__(self) -> Iterator[str]:
        return iter(self._by_key.values())


def split_known_jobs(jobs: list[dict], existing: UrlSet) -> tuple[list[dict], list[dict]]:
    """(new, already_known). Known jobs get their `url` rewritten to the stored
    spelling so the later last_seen touch hits the existing row. A posting
    scraped twice in one batch under two spellings is kept once (it would
    otherwise be classified twice and inserted as two rows). Jobs without a
    url stay in `new`, as before."""
    new: list[dict] = []
    known: list[dict] = []
    seen_new: set[str] = set()
    for job in jobs:
        url = job.get("url", "")
        stored = existing.stored(url) if url else None
        if stored is not None:
            if stored != url:
                job["url"] = stored
            known.append(job)
            continue
        if url:
            key = url_key(url)
            if key in seen_new:
                continue
            seen_new.add(key)
        new.append(job)
    return new, known


def duplicate_groups(rows: list[dict]) -> list[list[dict]]:
    """Groups of stored rows ({id, job_url, ...}) that are the same posting."""
    by_key: dict[str, list[dict]] = {}
    for r in rows:
        u = r.get("job_url") or ""
        if u:
            by_key.setdefault(url_key(u), []).append(r)
    return [g for g in by_key.values() if len(g) > 1]
