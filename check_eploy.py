"""Offline checks for the Eploy location extractor (label vs value) and the postback pager."""
import os
import sys

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")
import logging  # noqa: E402

logging.disable(logging.CRITICAL)
from bs4 import BeautifulSoup  # noqa: E402
import ats_scrapers as A  # noqa: E402

fails, n = [], 0


def check(cond, msg):
    global n
    n += 1
    if not cond:
        fails.append(msg)


def card(val: str) -> str:
    return (f'<div class="vsr-job-big"><h3><a href="22497/graduate-project-manager--leeds.html">GPM</a></h3>'
            f'<ul><li id="li_VacV_AllLocations_22497"><div class="label">All Locations:</div>'
            f'<div class="content"><span id="x_lblReadonlySelected">{val}</span></div></li></ul></div>')


for val, want in [("Leeds", "Leeds"), ("Not Specified", "Not Specified"), ("Cardiff, Bristol", "Cardiff, Bristol")]:
    a = BeautifulSoup(card(val), "html.parser").find("a")
    got, st = A._eploy_card_location(a)
    check(got == want and st == "extracted", f"{val!r} -> {got!r},{st}")
for val in ("", "All Locations:"):
    a = BeautifulSoup(card(val), "html.parser").find("a")
    got, st = A._eploy_card_location(a)
    check(got == "" and st == "marker_found_empty", f"empty {val!r} -> {got!r},{st}")
a = BeautifulSoup('<div><a href="1/x.html">x</a></div>', "html.parser").find("a")
check(A._eploy_card_location(a) == ("", "marker_not_found"), "no marker")

page = ('<form action="r.aspx"><input type="hidden" name="__VIEWSTATE" value="vs"><input name="q" value="">'
        '<a href="javascript:__doPostBack(&#39;ctl00$C$VacancyPager&#39;,&#39;2&#39;)">2</a></form>')
f = A._eploy_next_page_form(page, 1)
check(f is not None and f[0]["__EVENTTARGET"] == "ctl00$C$VacancyPager" and f[0]["__EVENTARGUMENT"] == "2"
      and f[0]["__VIEWSTATE"] == "vs", f"form {f}")
check(A._eploy_next_page_form(page, 2) is None, "no page 3 link")

import json  # noqa: E402

_off = {"offers": [{"title": "PM", "slug": "pm", "remote": True, "city": "Warsaw", "country": "Poland", "location": "Remote job",
                    "locations": [{"city": "Warsaw", "name": "Warsaw", "state": "Mazowieckie", "country": "Poland"}]},
                   {"title": "X", "slug": "x", "remote": True, "location": "Remote job", "locations": []}]}


class _R:
    def json(self):
        return _off


_orig = A._get_requests_sync
A._get_requests_sync = lambda *a, **k: _R()
_jobs = A.scrape_recruitee("t")
A._get_requests_sync = _orig
check(_jobs[0]["location"] == "Warsaw, Mazowieckie, Poland" and _jobs[0]["workplace_type"] == "Remote", f"recruitee {_jobs[0]['location']}")
check(_jobs[1]["location"] == "Remote", f"recruitee bare {_jobs[1]['location']}")

print(f"{n} checks, {len(fails)} failures")
for m in fails:
    print("FAIL", m)
sys.exit(1 if fails else 0)
