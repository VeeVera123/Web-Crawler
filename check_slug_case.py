"""Offline checks for slug_case.py and its wiring in supabase_handler.get_all_slugs.

Run: python check_slug_case.py

Simulates the real registry layout: archive_i shards by `id % 1000` computed in
the database, so the two spellings of a board (different ids) land in different
shards. The invariant under test: across ALL shards every board is scraped
exactly once, and no board is ever lost.
"""

import os
import sys

for k in ("SUPABASE_URL", "SUPABASE_KEY", "SUPABASE_SERVICE_KEY", "SUPABASE_SERVICE_ROLE_KEY"):
    os.environ.setdefault(k, "https://example.invalid" if k == "SUPABASE_URL" else "x")

import slug_case as sc  # noqa: E402
import supabase_handler as sh  # noqa: E402

failures = []
checks = 0


def check(cond, msg):
    global checks
    checks += 1
    if not cond:
        failures.append(msg)


# ── canonical_slug: allow-list only ──
for ats, raw, want in [
    ("workday", "summitracing|wd5|SummitRacing-Partner-External-Site", "summitracing|wd5|summitracing-partner-external-site"),
    ("ashby", "Tempo", "tempo"),
    ("smartrecruiters", "CITECH", "citech"),
    ("greenhouse", "ShiftFive", "shiftfive"),
    ("dayforce", "ipg|IPG", "ipg|ipg"),
    ("oracle_cloud_hcm", "ibtsjb.fa.ocs|CX_2", "ibtsjb.fa.ocs|cx_2"),
    ("paycom", "A490811F8035AE70DC4C2BD27F50067F", "a490811f8035ae70dc4c2bd27f50067f"),
    ("Workday", "Foo|wd1|Bar", "foo|wd1|bar"),  # ats compared case-insensitively
    # NOT folded: Lever is case-sensitive (ContactOut has jobs, contactout has none);
    # Taleo / PageUp / Paylocity are unverified; everything else is already lowercase-only
    ("lever", "ContactOut", "ContactOut"),
    ("taleo", "aa340|pca_external_Career_Site", "aa340|pca_external_Career_Site"),
    ("pageup", "532|Cawh", "532|Cawh"),
    ("paylocity", "6d8d47ac|JACQUET", "6d8d47ac|JACQUET"),
    ("bamboohr", "AcmeCo", "AcmeCo"),
    ("workday", "", ""),
]:
    check(sc.canonical_slug(ats, raw) == want, f"canonical_slug({ats!r}, {raw!r}) -> {sc.canonical_slug(ats, raw)!r}, want {want!r}")

# ── canonical_registry_rows: lowercases, de-duplicates the batch, keeps other fields ──
rows = [
    {"ats": "ashby", "slug": "Tempo", "source": "commoncrawl"},
    {"ats": "ashby", "slug": "tempo", "source": "seed"},
    {"ats": "ashby", "slug": "TEMPO", "source": "yc"},
    {"ats": "lever", "slug": "ContactOut", "source": "seed"},
    {"ats": "lever", "slug": "contactout", "source": "seed"},
    {"ats": "workday", "slug": "a|wd1|Site", "source": "seed"},
    {"ats": "greenhouse", "slug": "acme", "source": "seed"},
    {"ats": "ashby", "source": "seed"},  # no slug: passed through untouched
]
out = sc.canonical_registry_rows(rows)
keys = [(r["ats"], r.get("slug")) for r in out]
check(keys == [("ashby", "tempo"), ("lever", "ContactOut"), ("lever", "contactout"), ("workday", "a|wd1|site"),
               ("greenhouse", "acme"), ("ashby", None)], f"canonical_registry_rows: {keys}")
check(out[0]["source"] == "commoncrawl", "first spelling's other fields are kept")
check(rows[0]["slug"] == "Tempo", "input rows are not mutated")
check(len({(r["ats"], r.get("slug")) for r in out}) == len(out), "no duplicate (ats, slug) left in a batch")

# ── drop_case_twins ──
pairs = [("workday", "a|wd1|Site"), ("workday", "a|wd1|site"),   # twin -> drop mixed
         ("workday", "b|wd1|Only"),                              # no lowercase twin -> keep
         ("workday", "c|wd1|X"), ("workday", "c|wd1|x"),
         ("lever", "ContactOut"), ("lever", "contactout"),      # not allow-listed -> keep both
         ("taleo", "t|Cs"), ("taleo", "t|cs"),
         ("ashby", "Tempo"), ("greenhouse", "tempo"),            # same slug, other ATS: not a twin
         ("workday", "d|wd1|Y"), ("workday", "d|wd1|Z")]         # two mixed spellings, no lowercase: keep
present = {("workday", "a|wd1|site"), ("workday", "c|wd1|x"), ("lever", "contactout"), ("taleo", "t|cs")}
kept = sc.drop_case_twins(pairs, present)
check(("workday", "a|wd1|Site") not in kept and ("workday", "a|wd1|site") in kept, "mixed twin dropped, lowercase kept")
check(("workday", "b|wd1|Only") in kept, "mixed-case board with no lowercase twin is kept")
check(("lever", "ContactOut") in kept and ("lever", "contactout") in kept, "case-sensitive ATS untouched")
check(("taleo", "t|Cs") in kept, "unverified ATS untouched")
check(("ashby", "Tempo") in kept, "a lowercase row of ANOTHER ATS is not a twin")
check(("workday", "d|wd1|Y") in kept and ("workday", "d|wd1|Z") in kept, "twin-less mixed spellings both kept (nothing lost)")
check(len(kept) == len(pairs) - 2, f"exactly the two real twins dropped: {len(pairs) - len(kept)}")
check(sc.twin_candidates(pairs) == {
    "workday": ["a|wd1|site", "b|wd1|only", "c|wd1|x", "d|wd1|y", "d|wd1|z"], "ashby": ["tempo"]}, "twin_candidates")

# ── whole-registry simulation across shards (shard = id % 1000 % N, as archive_i_shard) ──
import random

rng = random.Random(7)
registry = []  # (id, ats, slug)
nid = 0
for n in range(3000):
    ats = rng.choice(["workday", "smartrecruiters", "ashby", "greenhouse", "lever", "taleo", "bamboohr", "paycom"])
    base = f"co{n}" if ats != "workday" else f"co{n}|wd{rng.randint(1, 5)}|Site{n}"
    spellings = {base.lower()}
    kind = rng.random()
    if ats in ("bamboohr",):
        pass
    elif kind < 0.45:
        spellings.add(base)                                  # lower + original (the common real shape)
    elif kind < 0.455:  # rare in production (13 groups of ~9,900): two mixed spellings, no lowercase one
        spellings = {base.upper() if ats != "workday" else base, base.title()}
    elif kind < 0.6:
        spellings = {base}                                   # lone mixed-case board
    for s in sorted(spellings):
        nid += rng.randint(1, 3)
        registry.append((nid, ats, s))
rng.shuffle(registry)
registry.sort()  # ids ascending, like ORDER BY id

N = 10
all_pairs = [(a, s) for _, a, s in registry]
by_id = {(a, s): i for i, a, s in registry}


def shard_rows(idx):
    return [(a, s) for i, a, s in registry if (i % 1000) % N == idx]


sh._lowercase_slugs_present = lambda ats, slugs: {s for s in slugs if (ats, s) in by_id}
scraped = []
for idx in range(N):
    scraped += sh._drop_duplicate_case_boards(shard_rows(idx))
check(len(scraped) == len(set(scraped)), "no (ats, slug) row scraped twice")
boards_total = {}
for a, s in all_pairs:
    boards_total.setdefault((a, s.lower() if sc.is_case_insensitive(a) else s), []).append(s)
boards_scraped = {}
for a, s in scraped:
    boards_scraped.setdefault((a, s.lower() if sc.is_case_insensitive(a) else s), []).append(s)
check(set(boards_scraped) == set(boards_total), "every board is still scraped by some shard (none lost)")
dups = {k: v for k, v in boards_scraped.items() if len(v) > 1 and sc.is_case_insensitive(k[0])}
twinless = {k for k, v in boards_total.items() if sc.is_case_insensitive(k[0]) and k[1] not in v}
check(all(k in twinless for k in dups), "the only boards still scraped twice are those with no lowercase spelling")
before = sum(len(v) - 1 for k, v in boards_total.items() if sc.is_case_insensitive(k[0]))
after = sum(len(v) - 1 for k, v in boards_scraped.items() if sc.is_case_insensitive(k[0]))
check(before > 100 and after < before * 0.1, f"duplicate scrapes reduced a lot: {before} -> {after}")
lever_before = sum(1 for a, s in all_pairs if a == "lever")
lever_after = sum(1 for a, s in scraped if a == "lever")
check(lever_before == lever_after, "case-sensitive ATS (lever): nothing skipped")

# full-table (fallback) path gives the same answer as the lookup path
full = set(all_pairs)
scraped_full = []
for idx in range(N):
    scraped_full += sh._drop_duplicate_case_boards(shard_rows(idx), full_table=full)
check(sorted(scraped_full) == sorted(scraped), "fallback (full table) path == RPC (lookup) path")

# ── lookup request is well formed, URL-encoded and chunked small ──
requests_seen = []


def fake_get(table, params="", limit=10000):
    requests_seen.append((table, params, limit))
    return []


sh._get = fake_get
import importlib
importlib.reload(sh)  # restore the real _lowercase_slugs_present
sh._get = fake_get
tricky = ['a|wd1|x', 'b"q', 'c,d', 'e)f', 'g h'] + [f"s{i}" for i in range(95)]
sh._lowercase_slugs_present("workday", tricky)
check(len(requests_seen) == 3, f"100 slugs -> 3 requests of <=40 ({len(requests_seen)})")
first = requests_seen[0][1]
check(first.startswith("select=slug&ats=eq.workday&slug=in.(%22a%7Cwd1%7Cx%22%2C%22b%5C%22q%22%2C%22c%2Cd%22"),
      f"slugs quoted + percent-encoded: {first[:120]}")
check(all(len(p) < 4000 for _, p, _ in requests_seen), "query strings stay far below the URL limit")

# ── fail open: a lookup error must not drop or lose anything ──
def boom(ats, slugs):
    raise sh.SupabaseFetchError("down")


sh._lowercase_slugs_present = boom
sample = [("workday", "a|wd1|Site"), ("workday", "a|wd1|site")]
check(sh._drop_duplicate_case_boards(sample) == sample, "lookup failure -> every spelling is scraped (fail open)")

# ── get_all_slugs end to end (RPC path) ──
importlib.reload(sh)
registry_by_shard = {0: [("workday", "a|wd1|Site"), ("ashby", "x")], 1: [("workday", "a|wd1|site")]}
sh._rpc = lambda fn, params, limit=1000: [{"ats": a, "slug": s} for a, s in registry_by_shard[params["p_shard_index"]]] if params["p_offset"] == 0 else []
sh._get = lambda table, params="", limit=10000: [{"slug": "a|wd1|site"}] if "ats=eq.workday" in params else []
check(sh.get_all_slugs(0, 2) == [("ashby", "x")], "shard 0 skips the mixed-case twin owned by shard 1")
check(sh.get_all_slugs(1, 2) == [("workday", "a|wd1|site")], "shard 1 keeps the lowercase board")

# ── every registry writer stores the canonical spelling ──
importlib.reload(sh)
posted = []


class _Ok:
    def raise_for_status(self):
        pass


sh.http_requests.post = lambda url, headers=None, json=None, timeout=None, params=None: posted.append((url, json)) or _Ok()
sh.http_requests.delete = lambda *a, **k: _Ok()
sh.resolve_oracle_slug("eeho|CX_1", "eeho.fa.us2|CX_1")
check(posted and posted[-1][1][0]["slug"] == "eeho.fa.us2|cx_1", f"resolve_oracle_slug stores lowercase: {posted[-1:]}")
posted.clear()
sh.populate_slug_registry([("workday", "A|wd1|Site"), ("workday", "a|wd1|site"), ("lever", "Foo")])
sent = [(r["ats"], r["slug"]) for _, rows in posted for r in rows]
check(sent == [("workday", "a|wd1|site"), ("lever", "Foo")], f"populate_slug_registry canonicalises + de-duplicates: {sent}")

# static guard: any module that upserts into archive_i on (ats, slug) must use the canonical helpers,
# so a new writer cannot quietly start creating case variants again
import glob
root = os.path.dirname(os.path.abspath(__file__))
for path in glob.glob(os.path.join(root, "**", "*.py"), recursive=True):
    name = os.path.relpath(path, root)
    if name.startswith("check_") or name == "slug_case.py":
        continue
    src = open(path, encoding="utf-8", errors="replace").read()
    writes = ("on_conflict" in src and "ats,slug" in src and ("archive_i" in src or "ARCHIVE_I_TABLE" in src))
    if writes:
        check("canonical_registry_rows" in src or "canonical_slug" in src,
              f"{name} upserts into archive_i but does not use slug_case's canonical helpers")

# ── discovery.upsert_to_supabase reports what REALLY went in (new vs already present vs merged) ──
os.environ.setdefault("SUPABASE_URL", "https://example.invalid")
os.environ.setdefault("SUPABASE_KEY", "x")
import datetime as _dt  # noqa: E402
import discovery  # noqa: E402

_now = _dt.datetime.now(_dt.timezone.utc)
_old = (_now - _dt.timedelta(days=30)).isoformat()
EXISTING = {("workday", "a|wd1|site"), ("greenhouse", "oldco")}  # already in archive_i


class _R:
    def __init__(self, rows):
        self._rows = rows

    def raise_for_status(self):
        pass

    def json(self):
        return self._rows


def _fake_post(url, headers=None, json=None, timeout=None, params=None):
    out = []
    for r in json:  # what PostgREST returns with return=representation: one row per input, first_seen per row
        seen = (r["ats"], r["slug"]) in EXISTING
        out.append({"ats": r["ats"], "slug": r["slug"], "first_seen": _old if seen else _dt.datetime.now(_dt.timezone.utc).isoformat()})
    return _R(out)


discovery.requests.post = _fake_post
discovery._drop_dead_cc_slugs = lambda d, label: {a: {s: n for s, n in (v.items() if isinstance(v, dict) else {x: "" for x in v}.items()) if s != "deadco"}
                                                  for a, v in d.items()}
discovery._INSERTED_THIS_RUN.clear()
st = {}
written = discovery.upsert_to_supabase(
    {"workday": {"A|wd1|Site", "a|wd1|site", "b|wd1|new"},   # two spellings of an existing board + one new
     "greenhouse": {"OldCo", "oldco", "newco", "deadco"}},    # a case twin of an existing slug, a new one, a dead one
    source="Github", stats=st)
wd, gh = st["by_ats"]["workday"], st["by_ats"]["greenhouse"]
check((wd["fetched"], wd["collapsed"], wd["written"], wd["new"], wd["present"]) == (3, 1, 2, 1, 1), f"workday accounting: {wd}")
check((gh["fetched"], gh["dead"], gh["collapsed"], gh["written"], gh["new"], gh["present"]) == (4, 1, 1, 2, 1, 1), f"greenhouse accounting: {gh}")
check(written == 4 and st["new"] == 2 and st["present"] == 2 and st["collapsed"] == 2 and st["dead"] == 1 and st["fetched"] == 7,
      f"totals: {written} {({k: v for k, v in st.items() if k != 'by_ats'})}")
# the same slug offered again by a LATER source in the same run is present, not new
st2 = {}
discovery.upsert_to_supabase({"workday": {"b|wd1|new"}}, source="Github", stats=st2)
check(st2["new"] == 0 and st2["present"] == 1, f"a slug inserted earlier in this run must not be new again: {st2}")
# dry run: nothing is claimed as new
st3 = {}
discovery.upsert_to_supabase({"workday": {"zz|wd1|x"}}, source="Github", dry_run=True, stats=st3)
check(st3["new"] is None and st3["written"] == 1, f"dry run: {st3}")

print(f"{checks - len(failures)}/{checks} checks passed")
for f in failures:
    print("FAIL:", f)
sys.exit(1 if failures else 0)
