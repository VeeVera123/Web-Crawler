"""Offline checks for job_url.py and its wiring (no network, no database).

Run: python check_job_url.py

The URL pairs are real rows from the `jobs` table:
  * SAME  = one posting stored twice (these showed up as duplicates in Notion)
  * DIFF  = look-alikes that are different postings and must stay separate
"""

import os
import sys

os.environ.setdefault("SUPABASE_URL", "https://example.invalid")
os.environ.setdefault("SUPABASE_KEY", "x")
os.environ.setdefault("SUPABASE_SERVICE_KEY", "x")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "x")

from job_url import UrlSet, canonical_job_url, duplicate_groups, split_known_jobs, url_key  # noqa: E402

failures = []
checks = 0


def check(cond, msg):
    global checks
    checks += 1
    if not cond:
        failures.append(msg)


SAME = [
    # board-slug case (Ashby)
    ("https://jobs.ashbyhq.com/ashby/1e6bf4c7-6452-418b-9277-a28eb8a6f218",
     "https://jobs.ashbyhq.com/Ashby/1e6bf4c7-6452-418b-9277-a28eb8a6f218"),
    ("https://jobs.ashbyhq.com/Blacksmith%20Agency/d753234d-31a1-48cc-94d4-cccae87226cc",
     "https://jobs.ashbyhq.com/blacksmith%20agency/d753234d-31a1-48cc-94d4-cccae87226cc"),
    ("https://jobs.ashbyhq.com/aven/724a3f08-34d3-4305-8135-ea3236224766",
     "https://jobs.ashbyhq.com/Aven/724a3f08-34d3-4305-8135-ea3236224766/"),
    # board-slug case (Workday)
    ("https://acu.wd108.myworkdayjobs.com/acucareers/job/Remote/Account-Manager---Education-Partnerships_JR101170",
     "https://acu.wd108.myworkdayjobs.com/ACUCareers/job/Remote/Account-Manager---Education-Partnerships_JR101170"),
    ("https://geha.wd5.myworkdayjobs.com/GEHACareers/job/Remote/Growth-Activation-and-Operations-Specialist_R-005331",
     "https://geha.wd5.myworkdayjobs.com/gehacareers/job/Remote/Growth-Activation-and-Operations-Specialist_R-005331"),
    ("https://geha.wd5.myworkdayjobs.com/en-US/GEHACareers/job/Remote/X_R-1",
     "https://geha.wd5.myworkdayjobs.com/en-US/gehacareers/job/Remote/X_R-1"),
    # trailing slash / host case / scheme-relative noise
    ("https://empcloud.com/project-management/", "https://empcloud.com/project-management"),
    ("https://EmpCloud.com/project-management", "https://empcloud.com/project-management/"),
    # tracking params
    ("https://www.linkedin.com/jobs/view/territory-account-manager-at-equipmentshare-4473914039"
     "?position=16&pageNum=0&refId=2%2FSGKORoj1zaH5x%2Bkxyj9w%3D%3D&trackingId=89JpBMX90SJ94OfpAk6Ehw%3D%3D",
     "https://www.linkedin.com/jobs/view/territory-account-manager-at-equipmentshare-4473914039"
     "?position=13&pageNum=0&refId=9gt9i%2Bq6iCdFjYN3p1bGHg%3D%3D&trackingId=gIk2gfZbn3pj48ckbQpQZg%3D%3D"),
    ("https://jobs.lever.co/acme/123e4567-e89b-12d3-a456-426614174000?lever-source=LinkedIn",
     "https://jobs.lever.co/acme/123e4567-e89b-12d3-a456-426614174000"),
    ("https://boards.greenhouse.io/acme/jobs/123?gh_jid=123&utm_source=x&utm_medium=y",
     "https://boards.greenhouse.io/acme/jobs/123?gh_jid=123"),
    # query parameter order / name case
    ("https://x.example/p?a=1&gh_jid=5", "https://x.example/p?gh_jid=5&a=1"),
    ("https://workforcenow.adp.com/r.html?cid=c&jobId=5", "https://workforcenow.adp.com/r.html?jobid=5&cid=c"),
    # percent-escape hex case
    ("https://jobs.ashbyhq.com/a%2fb/1", "https://jobs.ashbyhq.com/a%2Fb/1"),
]

DIFF = [
    # different gh_jid on the same landing page
    ("https://stripe.com/jobs/search?gh_jid=7230921", "https://stripe.com/jobs/search?gh_jid=8175824"),
    ("https://www.assembly.health/careers?gh_jid=5208093008#jobs",
     "https://www.assembly.health/careers?gh_jid=5363047008#jobs"),
    # different ADP / SuccessFactors / Taleo job ids
    ("https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=5eed0337&ccId=1&jobId=589951",
     "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid=5eed0337&ccId=1&jobId=589950"),
    ("https://career2.successfactors.eu/sfcareer/jobreqcareer?jobId=56788&company=hsbcholdin",
     "https://career2.successfactors.eu/sfcareer/jobreqcareer?jobId=40199&company=pandoraas"),
    ("https://career2.successfactors.eu/sfcareer/jobreqcareer?jobId=5&company=hsbcholdin",
     "https://career2.successfactors.eu/sfcareer/jobreqcareer?jobId=5&company=pandoraas"),
    ("https://hccs.taleo.net/careersection/3/jobdetail.ftl?job=26002EP",
     "https://hccs.taleo.net/careersection/5/jobdetail.ftl?job=26002EP"),
    ("https://baesystems.taleo.net/careersection/2/jobdetail.ftl?job=00128927",
     "https://baesystems.taleo.net/careersection/2/jobdetail.ftl?job=00128926"),
    # single-page career sites that distinguish jobs by #fragment
    ("https://fermacorp.com/careers/#50", "https://fermacorp.com/careers/#1464"),
    # query VALUES keep their case (base64 ids)
    ("https://vitalspace.co.in/career-details.php?id=NDE=", "https://vitalspace.co.in/career-details.php?id=NDA="),
    ("https://x.example/d?id=AbC=", "https://x.example/d?id=abc="),
    ("https://powertofly.com/jobs/?primary_skills=Operations+Management",
     "https://powertofly.com/jobs/?primary_skills=Customer+Engagement"),
    ("https://recruitingbypaycor.com/career/JobIntroduction.action?clientId=a&id=8a78879e&source=&lang=en",
     "https://recruitingbypaycor.com/career/JobIntroduction.action?clientId=a&id=8a78839f&source=&lang=en"),
    # two different Ashby companies / postings
    ("https://jobs.ashbyhq.com/ashby/1e6bf4c7-6452-418b-9277-a28eb8a6f218",
     "https://jobs.ashbyhq.com/aven/1e6bf4c7-6452-418b-9277-a28eb8a6f218"),
    # different hosts
    ("https://jobs.lever.co/acme/1", "https://jobs.lever.co/acme2/1"),
]

for a, b in SAME:
    check(url_key(a) == url_key(b), f"SAME pair got different keys:\n    {a}\n    {b}")
for a, b in DIFF:
    check(url_key(a) != url_key(b), f"DIFF pair got the same key:\n    {a}\n    {b}")

# canonical form: stable, idempotent, and equal for the pairs a race would produce
for a, b in SAME:
    check(canonical_job_url(canonical_job_url(a)) == canonical_job_url(a), f"canonical not idempotent: {a}")
for a, b in SAME[:6]:  # board-slug-case pairs: both shards must write the SAME string
    check(canonical_job_url(a) == canonical_job_url(b), f"canonical differs for slug-case pair:\n    {a}\n    {b}")
check(canonical_job_url("https://jobs.ashbyhq.com/Ashby/1E6BF4C7-6452-418B-9277-A28EB8A6F218")
      == "https://jobs.ashbyhq.com/ashby/1e6bf4c7-6452-418b-9277-a28eb8a6f218", "ashby canonical form")
# Workday: only the site slug is folded; the posting slug keeps the case the ATS emitted
check(canonical_job_url("https://acu.wd108.myworkdayjobs.com/ACUCareers/job/Remote/Acct-Mgr_JR101170")
      == "https://acu.wd108.myworkdayjobs.com/acucareers/job/Remote/Acct-Mgr_JR101170", "workday canonical form")
# identity-bearing parts survive
check("gh_jid=5" in canonical_job_url("https://x.example/p/?gh_jid=5&utm_source=a"), "gh_jid kept")
check(canonical_job_url("https://fermacorp.com/careers/#50").endswith("#50"), "fragment kept")
check(canonical_job_url("https://x.example/d?id=AbC=") == "https://x.example/d?id=AbC=", "query value case kept")
check(canonical_job_url("https://technixtechnology.com/job-detail.php?openingID= 60")
      == "https://technixtechnology.com/job-detail.php?openingID= 60", "unusual query untouched")
for odd in ("", "not a url", "mailto:a@b.c"):
    check(canonical_job_url(odd) == odd and url_key(odd) == odd, f"non-url passthrough: {odd!r}")

# UrlSet: identity membership + remembers the stored spelling
stored = "https://jobs.ashbyhq.com/Ashby/1e6bf4c7-6452-418b-9277-a28eb8a6f218"
scraped = "https://jobs.ashbyhq.com/ashby/1e6bf4c7-6452-418b-9277-a28eb8a6f218"
us = UrlSet([stored])
check(scraped in us and stored in us and len(us) == 1, "UrlSet membership by identity")
check(us.stored(scraped) == stored, "UrlSet.stored returns the stored spelling")
us.add(scraped)
check(len(us) == 1 and us.stored(scraped) == stored, "UrlSet keeps the first spelling")
check("https://jobs.ashbyhq.com/other/1" not in us and "" not in us, "UrlSet non-member")
check(sorted(us) == [stored], "UrlSet iterates stored spellings")

# split_known_jobs: known -> url rewritten to stored; same posting twice in a batch -> once
jobs = [{"url": scraped, "title": "a"}, {"url": "https://x.example/new"}, {"url": "https://X.example/new/"}, {"title": "no url"}]
new, known = split_known_jobs(jobs, UrlSet([stored]))
check([j["url"] for j in known] == [stored], "known job url rewritten to stored spelling")
check(len(new) == 2 and new[0]["url"] == "https://x.example/new" and "url" not in new[1],
      f"batch duplicates collapsed, url-less kept: {new}")

# duplicate_groups
rows = [{"id": 1, "job_url": stored}, {"id": 2, "job_url": scraped}, {"id": 3, "job_url": "https://x.example/1"}]
check([[r["id"] for r in g] for g in duplicate_groups(rows)] == [[1, 2]], "duplicate_groups")

# ── wiring: add_jobs_batch must not insert a twin, and must touch the STORED url ──
import supabase_handler as sh  # noqa: E402

posts = []


class _Resp:
    def raise_for_status(self):
        pass

    def json(self):
        return []


def fake_post(url, headers=None, json=None, timeout=None, params=None):
    posts.append(json)
    return _Resp()


sh.http_requests.post = fake_post
existing = UrlSet([stored])
job_known = {"url": scraped, "title": "Impl", "company": "Ashby", "source_ats": "Ashby"}
job_new = {"url": "https://jobs.ashbyhq.com/NewCo/AAAAAAAA-0000-0000-0000-000000000001", "title": "T",
           "company": "NewCo", "source_ats": "Ashby"}
added, _ = sh.add_jobs_batch([job_known, job_new], ["x", "x"], existing_urls=existing)
inserted = [r["job_url"] for batch in posts for r in batch]
check(added == 1, f"only the genuinely new job is added (got {added})")
check(stored in inserted and scraped not in inserted,
      f"known job touched under its stored url, no twin written: {inserted}")
check("https://jobs.ashbyhq.com/newco/aaaaaaaa-0000-0000-0000-000000000001" in inserted,
      f"new Ashby row stored in canonical spelling: {inserted}")
check(len(inserted) == len(set(inserted)) == 2, "no duplicate rows in the same upsert")

# in-batch twins (two shards' spellings in one batch) insert once
posts.clear()
a = {"url": "https://acu.wd108.myworkdayjobs.com/acucareers/job/R/X_JR1", "title": "t", "company": "c", "source_ats": "Workday"}
b = {"url": "https://acu.wd108.myworkdayjobs.com/ACUCareers/job/R/X_JR1", "title": "t", "company": "c", "source_ats": "Workday"}
added, _ = sh.add_jobs_batch([a, b], ["x", "x"], existing_urls=UrlSet())
check(added == 1, f"in-batch twins inserted once (got {added})")

# a plain set from an older caller still works
posts.clear()
added, _ = sh.add_jobs_batch([a], ["x"], existing_urls={a["url"]})
check(added == 0, "plain-set existing_urls still honoured")

# ── duplicate sweep: keeps tracked/first row, vetoes the rest, refuses a runaway ──
patched = []
sh._patch = lambda table, filters, data: patched.append((filters, data)) or True


def run_sweep(rows, **kw):
    patched.clear()
    sh._get = lambda table, params="", limit=10000: rows if "offset=0" in params else []
    return sh.mark_duplicate_jobs_vetoed(**kw)


base = [{"id": i, "job_url": f"https://x.example/{i}", "application_status": "not_applied", "is_active": True}
        for i in range(100, 160)]
pair = [{"id": 1, "job_url": stored, "application_status": "not_applied", "is_active": True},
        {"id": 2, "job_url": scraped, "application_status": "not_applied", "is_active": True}]
res = run_sweep(pair + base)
check(res == {"groups": 1, "vetoed": 1} and patched == [("id=in.(2)", {"clearance": "vetoed", "is_active": False})],
      f"sweep vetoes the later twin: {res} {patched}")
# the user is tracking the later twin: it is kept, the other (untracked) twin goes
pair_tracked = [dict(pair[0]), dict(pair[1], application_status="applied")]
res = run_sweep(pair_tracked + base)
check(patched == [("id=in.(1)", {"clearance": "vetoed", "is_active": False})], f"tracked twin kept: {patched}")
# both tracked -> nothing touched
res = run_sweep([dict(r, application_status="applied") for r in pair] + base)
check(not patched, "two tracked twins: nothing vetoed")
# inactive first row loses to the active one
res = run_sweep([dict(pair[0], is_active=False), dict(pair[1])] + base)
check(patched == [("id=in.(1)", {"clearance": "vetoed", "is_active": False})], f"active twin preferred: {patched}")
# no duplicates -> no writes
res = run_sweep(base)
check(res["groups"] == 0 and not patched, "no duplicates, no writes")
# runaway guard: > max_fraction of the table would be removed -> do nothing
res = run_sweep(pair + base[:3])
check(res["vetoed"] == 0 and not patched, "runaway guard refuses to act")

print(f"{checks - len(failures)}/{checks} checks passed")
for f in failures:
    print("FAIL:", f)
sys.exit(1 if failures else 0)
