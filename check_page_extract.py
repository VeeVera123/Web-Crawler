"""Offline checks for page_extract.py (Crawl II's HTML -> job helpers) and its crawl_ii.py wiring.
No network. Run: python check_page_extract.py"""
import os
import sys

for _k in ("SUPABASE_URL", "SUPABASE_KEY", "CEREBRAS_API_KEY", "GROQ_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"):
    os.environ.setdefault(_k, "x")
import logging  # noqa: E402

logging.disable(logging.CRITICAL)
import json  # noqa: E402
import page_extract as P  # noqa: E402

fails, n = [], 0


def check(cond, msg):
    global n
    n += 1
    if not cond:
        fails.append(msg)


JD = """<html><head><title>Senior Customer Success Manager | Acme</title></head><body>
<nav><a href="/">Home</a><a href="/careers">Careers</a><a href="/apply">Apply now</a></nav>
<div class="cookie-banner">We use cookies. Accept all cookies. Manage consent preferences.</div>
<main><h1>Senior Customer Success Manager</h1>
<aside><p>Location: Remote - Worldwide</p><p>Employment type: Full-time</p><p>Salary: $90k</p></aside>
<h2>About the role</h2><p>You will own onboarding and renewals for our enterprise accounts and you'll partner with sales.</p>
<h2>Responsibilities</h2><ul><li>Run QBRs</li><li>Drive adoption</li><li>Reduce churn</li><li>Forecast renewals</li></ul>
<h2>Requirements</h2><ul><li>5 years in CS</li><li>Strong communication</li><li>SaaS experience</li><li>Data fluency</li></ul>
<p>Acme is an equal opportunity employer. Apply now to join us. Your application will be reviewed within a week and you will hear back.</p>
</main><footer>Offices: London, UK. Privacy. Terms. Follow us on LinkedIn.</footer></body></html>"""
text, li = P.main_text(JD)
check("cookies" not in text.lower() and "Offices: London" not in text and "Privacy" not in text, f"boilerplate removed: {text[:120]!r}")
check("Responsibilities" in text and "Requirements" in text and "onboarding and renewals" in text, "JD body kept")
check("Location: Remote - Worldwide" in text and "Employment type" in text, "job-meta <aside> kept")
check(P.is_job_description(text, li), f"real JD passes the gate: {P.jd_features(text, li)}")
check(P.page_title(JD) == "Senior Customer Success Manager", f"page_title {P.page_title(JD)!r}")

# an ASP.NET page-wide <form class="menu-wrapper"> must NOT be removed (safeguard) ----------------------------
ASPX = ('<html><body><form id="f" class="main-menu-form"><div><h1>Operations Manager</h1><h2>Responsibilities</h2>'
        '<p>' + "You will run daily operations and report to the COO. " * 12 + '</p><h2>Requirements</h2><p>' +
        "Five years of experience required. Apply now. " * 6 + '</p></div></form></body></html>')
t2, _ = P.main_text(ASPX)
check("Responsibilities" in t2 and "Requirements" in t2, "page-wide flagged <form> not removed by the safeguard")

# not-a-JD pages --------------------------------------------------------------------------------------------
MARKETING = ("<html><body><main><h1>Why work with us</h1><p>" + "Our culture is built on trust and we love our customers. " * 20 +
             "</p><p>Benefits include free lunch.</p></main></body></html>")
t3, l3 = P.main_text(MARKETING)
check(not P.is_job_description(t3, l3), f"marketing page rejected {P.jd_features(t3, l3)}")
CLOSED = ("<html><body><main><h1>Customer Success Manager</h1><p>Sorry, this position has been filled and is no longer "
          "accepting applications. Responsibilities Requirements Qualifications apply now. " + "x " * 200 + "</p></main></body></html>")
t4, l4 = P.main_text(CLOSED)
check(not P.is_job_description(t4, l4), "closed/expired posting rejected")
check(P.single_job_page(CLOSED, "https://a.com/j", "a") is None, "closed page is not a single job")
LISTING = "<html><body><main>" + "".join(f"<div><a href='/j/{i}'>Role {i}</a> Apply now</div>" for i in range(10)) + "x " * 300 + "</main></body></html>"
t5, l5 = P.main_text(LISTING)
check(not P.is_job_description(t5, l5), "listing page (many Apply buttons) rejected")

# single job page --------------------------------------------------------------------------------------------
sj = P.single_job_page(JD, "https://acme.com/careers/senior-csm", "acme")
check(sj is not None and sj["title"] == "Senior Customer Success Manager" and sj["url"].endswith("senior-csm"), f"single job {sj and sj['title']}")
LAND = JD.replace("Senior Customer Success Manager", "Join the Acme Team").replace("<title>Join the Acme Team | Acme</title>", "")
check(P.single_job_page(LAND, "https://acme.com/careers", "acme") is None, "'Join the team' landing page is not a single job")
check(P.single_job_page(JD.replace("Senior Customer Success Manager", "We're hiring: Account Manager"), "https://a.com/x", "a")["title"] == "Account Manager",
      "'We're hiring:' prefix stripped")

# embedded state JSON ----------------------------------------------------------------------------------------
NEXT = ('<html><body><script id="__NEXT_DATA__" type="application/json">' + json.dumps({"props": {"pageProps": {"jobs": [
    {"title": "Customer Success Manager", "slug": "csm", "absolute_url": "https://x.com/jobs/1", "location": {"name": "Remote, Worldwide"},
     "department": {"name": "CS"}, "description": "<p>Own renewals</p>"},
    {"title": "Account Manager", "url": "/jobs/2", "locations": [{"city": "Berlin", "country": "Germany"}], "employmentType": "FULL_TIME"}],
    "nav": [{"title": "Home", "url": "/"}, {"title": "About", "url": "/about"}]}}}) + '</script></body></html>')
sj_ = P.extract_state_jobs(NEXT, "https://x.com/careers", "x")
check(len(sj_) == 2 and sj_[0]["location"] == "Remote, Worldwide" and sj_[1]["url"] == "https://x.com/jobs/2" and "Berlin" in sj_[1]["location"],
      f"__NEXT_DATA__ jobs: {[(j['title'], j['location']) for j in sj_]}")
check(all(j["title"] not in ("Home", "About") for j in sj_), "nav {title,url} arrays are not jobs")
WIN = '<script>window.__INITIAL_STATE__ = {"openings":[{"jobTitle":"Project Manager","applyUrl":"/apply/9","city":"Lisbon","country":"Portugal"}]};</script>'
w = P.extract_state_jobs(WIN, "https://y.com/c", "y")
check(len(w) == 1 and w[0]["location"] == "Lisbon, Portugal", f"window.__X__ assignment: {w}")
WIN2 = "<script>var data = {\"jobs\":[{\"title\":\"Ops Manager\",\"url\":\"/o/1\",\"location\":\"Remote\"}],\"s\":\"a});b\"}; foo();</script>"
check(len(P.extract_state_jobs(WIN2, "https://y.com/c", "y")) == 1, "balanced parse survives '});' inside a string")
ZOHO = ('<input type="hidden" value="[{&#34;Remote_Job&#34;:true,&#34;Posting_Title&#34;:&#34;Head of Ops&#34;,&#34;id&#34;:&#34;77&#34;,'
        '&#34;City&#34;:null,&#34;Publish&#34;:true,&#34;Job_Description&#34;:&#34;It&#39;s great&#34;}]" id="jobs">')
z = P.extract_state_jobs(ZOHO, "https://careers.acme.com/jobs/Careers", "acme")
check(len(z) == 1 and z[0]["url"] == "https://careers.acme.com/jobs/Careers/77" and z[0]["workplace_type"] == "Remote", f"zoho hidden input: {z}")
BLOG = ('<script type="application/json">' + json.dumps({"posts": [{"title": "How to hire", "url": "/blog/1", "created_at": "2026"},
                                                                    {"title": "Q3 news", "url": "/blog/2", "created_at": "2026"}]}) + '</script>')
check(P.extract_state_jobs(BLOG, "https://z.com/", "z") == [], "blog post list is not jobs")

# microdata ---------------------------------------------------------------------------------------------------
MD = ('<div itemscope itemtype="https://schema.org/JobPosting"><h2 itemprop="title">Program Manager</h2>'
      '<a itemprop="url" href="/jobs/pm">link</a><span itemprop="addressLocality">Austin</span>, <span itemprop="addressRegion">TX</span>'
      '<div itemprop="description">Lead programs.</div><meta itemprop="jobLocationType" content="TELECOMMUTE"></div>')
m = P.extract_microdata_jobs(MD, "https://a.com/c", "a")
check(len(m) == 1 and m[0]["location"] == "Austin, TX" and m[0]["workplace_type"] == "Remote" and m[0]["url"] == "https://a.com/jobs/pm", f"microdata {m}")

# frames --------------------------------------------------------------------------------------------------------
FR = ('<iframe src="https://www.googletagmanager.com/ns.html?id=GTM-1"></iframe><iframe src="//www.youtube.com/embed/x"></iframe>'
      '<iframe data-src="https://app.trinethire.com/companies/1-acme"></iframe><iframe src="/jobs/embed"></iframe>'
      '<iframe src="https://www2.acme.com/l/1/form.html"></iframe><script src="/wp-includes/js/jobs.js"></script>')
fr = P.find_job_frames(FR, "https://www.acme.com/careers")
check("https://app.trinethire.com/companies/1-acme" in fr and "https://www.acme.com/jobs/embed" in fr, f"frames {fr}")
check(not any("googletagmanager" in u or "youtube" in u or "form.html" in u or "wp-includes" in u for u in fr), f"junk frames skipped {fr}")

# feeds / wordpress ------------------------------------------------------------------------------------------------
RSS = ('<rss><channel><item><title><![CDATA[Customer Success Manager]]></title><link>https://a.com/job/1</link>'
       '<description><![CDATA[<p>Own &amp; grow accounts</p>]]></description></item></channel></rss>')
f = P.parse_feed_jobs(RSS, "https://a.com/feed", "a")
check(len(f) == 1 and f[0]["title"] == "Customer Success Manager" and "Own & grow accounts" in f[0]["description"], f"rss {f}")
check(P.find_feed_links('<link rel="alternate" type="application/rss+xml" title="Jobs" href="/jobs/feed">', "https://a.com/") == ["https://a.com/jobs/feed"], "job feed link")
check(P.find_feed_links('<link rel="alternate" type="application/rss+xml" title="Blog" href="/feed">', "https://a.com/") == [], "blog feed ignored")
check(P.wp_api_root('<link rel="https://api.w.org/" href="https://a.com/wp-json/" />', "https://a.com/") == "https://a.com/wp-json/", "wp api root")
check(P.wp_job_endpoints({"post": {"slug": "post", "rest_base": "posts"}, "job_listing": {"slug": "job_listing", "rest_base": "job-listings", "name": "Jobs"}}) == ["job-listings"], "wp job endpoint")
wp = P.parse_wp_posts([{"title": {"rendered": "Ops Lead &amp; PM"}, "link": "https://a.com/job/ops", "content": {"rendered": "<p>Do things</p>"}, "meta": {"_job_location": "Remote"}}], "a")
check(wp and wp[0]["title"] == "Ops Lead & PM" and wp[0]["location"] == "Remote", f"wp posts {wp}")

# charset ---------------------------------------------------------------------------------------------------------
check(P.decode_html("<html><meta charset='iso-8859-1'>Düsseldorf".encode("latin-1")) .endswith("Düsseldorf"), "meta charset latin-1")
check(P.decode_html("Düsseldorf".encode("cp1252")) == "Düsseldorf", "invalid utf-8 falls back to cp1252")
check(P.decode_html("Düsseldorf".encode("utf-8"), "text/html; charset=UTF-8") == "Düsseldorf", "header charset utf-8")
check(P.decode_html("日本".encode("shift_jis"), "text/html; charset=Shift_JIS") == "日本", "header charset shift_jis")

# crawl_ii wiring ------------------------------------------------------------------------------------------------
import crawl_ii as C  # noqa: E402

check(C._is_generic_anchor("View position") and C._is_generic_anchor("Read more") and C._is_generic_anchor("Angebot ansehen"), "generic anchors")
check(not C._is_generic_anchor("Customer Success Manager"), "real title is not generic")
check(C._worth_detail_fetch("Senior Customer Success Manager") and not C._worth_detail_fetch("Staff Backend Engineer"), "role prefilter")
check(C._worth_detail_fetch("View position"), "generic anchor still fetched")
job = C._confirm_and_build_posting(JD, {"title": "View position", "url": "https://acme.com/j/1"}, "acme")
check(job and job["title"] == "Senior Customer Success Manager" and "cookies" not in job["description"].lower(), f"confirm uses h1 title + clean text: {job and job['title']}")
check(job and "Remote" in job["location"], f"location read from kept meta aside: {job and job['location']!r}")
check(C._confirm_and_build_posting(MARKETING, {"title": "Why us", "url": "https://acme.com/why"}, "acme") is None, "marketing page not confirmed")
LD = ('<html><script type="application/ld+json">' + json.dumps({"@type": "JobPosting", "title": "PM", "jobLocationType": "TELECOMMUTE",
      "applicantLocationRequirements": {"name": "Portugal"}, "description": "<p>Lead</p>", "url": "https://a.com/j"}) + '</script></html>')
lj = C._extract_jsonld_jobs(LD, "https://a.com/c", "a")
check(lj and lj[0]["workplace_type"] == "Remote", f"json-ld TELECOMMUTE -> workplace_type {lj}")

# sitemap ------------------------------------------------------------------------------------------------------
ch, pg = P.parse_sitemap("<sitemapindex><sitemap><loc>https://a.com/job-sitemap.xml</loc></sitemap><sitemap><loc>https://a.com/post-sitemap.xml</loc></sitemap></sitemapindex>")
check(ch and not pg and P.pick_job_sitemaps(ch) == ["https://a.com/job-sitemap.xml"], "sitemap index -> job child only")
cands = P.sitemap_job_candidates(["https://a.com/jobs/customer-success-manager-12", "https://a.com/jobs/", "https://a.com/about-us", "https://a.com/blog/jobs-report"])
check([c["title"] for c in cands] == ["Customer Success Manager"], f"sitemap job candidates {cands}")
check(P.slug_title("https://a.com/job/1234/project-manager.html") == "Project Manager", "slug title strips ids/extension")

print(f"page_extract checks: {n - len(fails)}/{n} passed")
for m in fails:
    print("FAIL", m)
sys.exit(1 if fails else 0)
