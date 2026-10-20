"""
Pure (no network, no DB) HTML -> job-data helpers for Crawl II (crawl_ii.py).

Everything here takes a string of HTML and returns plain data, so it can be unit-tested
offline (check_page_extract.py) and run inside crawl_ii's parse thread-pool.

Techniques, and where the idea comes from (surveyed 2026-10 from open-source extractors:
html2rss' AutoSource scrapers [Schema / Microdata / JsonState / WordpressApi / SemanticHtml /
LinkHeuristics], scrapinghub/extruct [JSON-LD + microdata + RDFa], trafilatura / readability /
jusText [boilerplate removal by tag + link/text density], Firecrawl's onlyMainContent, and the
embedded-state tricks used by Next.js/Nuxt scrapers):

  main_text()            description = the job's own content, not nav / footer / cookie banner /
                         "related jobs". Safeguarded so it can only ever drop boilerplate.
  jd_score()             "does this look like a job description?" -- section headings, second-person
                         duties, bullets, apply call-to-action, EEO text, job-meta labels.
  extract_microdata_jobs schema.org JobPosting written as itemscope/itemprop markup.
  extract_state_jobs     job arrays inside __NEXT_DATA__ / __NUXT__ / window.__X__ = {...} /
                         <script type=application/json> / data-* props / hidden <input value=json>
                         (this last one is how Zoho Recruit custom domains ship their jobs).
  find_job_frames        iframes / frames / embeds / loader scripts that carry the real job board.
  find_feed_links, parse_feed_jobs, wp_*   RSS/Atom feeds and the WordPress REST API.
  single_job_page        the page itself IS one posting (no listing, no JSON-LD).
  decode_html            charset-aware decoding (header, BOM, <meta charset>) instead of utf-8-only.
"""

from __future__ import annotations

import html as html_lib
import json
import math
import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse

from selectolax.lexbor import LexborHTMLParser

# ── charset-aware decoding ────────────────────────────────────────────────────

_META_CHARSET_RE = re.compile(rb'<meta[^>]+charset\s*=\s*["\']?\s*([A-Za-z0-9_\-:.]+)', re.I)
_HEADER_CHARSET_RE = re.compile(r'charset\s*=\s*["\']?([A-Za-z0-9_\-:.]+)', re.I)


def decode_html(raw: bytes, content_type: str = "") -> str:
    """bytes -> str. Order: BOM, Content-Type charset, <meta charset> in the first 4KB, then UTF-8,
    then cp1252 as the last resort (a page that is not valid UTF-8 is almost always Windows-1252 /
    Latin-1, which is why "Düsseldorf" used to arrive as mojibake or lost characters)."""
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:].decode("utf-8", errors="replace")
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return raw.decode("utf-16", errors="replace")
        except Exception:
            pass
    declared = ""
    m = _HEADER_CHARSET_RE.search(content_type or "")
    if m:
        declared = m.group(1)
    else:
        m2 = _META_CHARSET_RE.search(raw[:4096])
        if m2:
            declared = m2.group(1).decode("ascii", errors="ignore")
    if declared:
        try:
            return raw.decode(declared.strip(), errors="replace")
        except LookupError:
            pass
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


# ── title ─────────────────────────────────────────────────────────────────────

_GENERIC_TITLE_RE = re.compile(
    r"^(careers?|jobs?|join (our|the) team|join us|work (with|for) us|open (positions?|roles?|jobs?)|"
    r"current (openings?|vacancies)|vacancies|opportunities|employment|we('| a)re hiring|home|"
    r"career opportunities|job openings?|apply( now)?|404.*|page not found|not found)$", re.I)


def page_title(html: str) -> str:
    """Best title for a single-posting page: first <h1>, else og:title, else <title> minus site suffix."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return ""
    h1 = tree.css_first("h1")
    if h1 is not None:
        t = re.sub(r"\s+", " ", h1.text(deep=True, separator=" ", strip=True))
        if 3 <= len(t) <= 160:
            return t
    og = tree.css_first('meta[property="og:title"]')
    if og is not None and og.attributes.get("content"):
        t = html_lib.unescape(og.attributes["content"]).strip()
        if t:
            return re.split(r"\s+[|–—]\s+|\s+-\s+(?=[A-Z])", t)[0].strip()[:160]
    tt = tree.css_first("title")
    if tt is not None:
        t = re.sub(r"\s+", " ", tt.text(strip=True))
        return re.split(r"\s+[|–—]\s+|\s+-\s+(?=[A-Z])", t)[0].strip()[:160]
    return ""


def is_generic_title(title: str) -> bool:
    return not title or bool(_GENERIC_TITLE_RE.match(title.strip()))


# ── "does this look like a job description?" ─────────────────────────────────

_JD_HEADING_RE = re.compile(
    r"\b(responsibilit(?:y|ies)|requirements?|qualifications?|what you(?:'|’)?ll (?:do|be doing|bring|need)|"
    r"what you will (?:do|be doing|bring|need)|about the (?:role|job|position|team|opportunity)|about this (?:role|job|position)|"
    r"who you are|what we(?:'|’)?re looking for|what we are looking for|what we offer|what(?:'|’)?s in it for you|"
    r"your (?:role|responsibilit(?:y|ies)|profile|skills|background|impact|mission)|key (?:duties|responsibilit(?:y|ies)|accountabilit(?:y|ies))|"
    r"duties|essential (?:functions|duties)|job (?:description|summary|purpose|overview)|position (?:summary|overview|description)|"
    r"role (?:overview|summary|description)|nice to have|preferred (?:skills|qualifications)|ideal candidate|"
    r"minimum qualifications|basic qualifications|skills (?:and|&) (?:experience|qualifications)|"
    r"experience (?:and|&) skills|why (?:join|work)|we are looking for|we(?:'|’)?re looking for|"
    r"you will|you(?:'|’)?ll|you have|you are|you bring|benefits|perks|"
    r"stellenbeschreibung|aufgaben|anforderungen|dein profil|ihr profil|ihre aufgaben|"
    r"missions?|profil recherché|vos missions|descripción del puesto|requisitos|funciones|"
    r"descrição da vaga|responsabilidades|requisitos)\b", re.I)
_JD_APPLY_RE = re.compile(
    r"\b(apply now|apply today|apply for this|apply online|apply here|how to apply|submit (?:your )?(?:an )?application|"
    r"send (?:us )?your (?:cv|resum[eé]|application)|easy apply|apply with|"
    r"jetzt bewerben|bewirb dich|postuler|postulez|aplicar|candidatar)\b", re.I)
_JD_META_RE = re.compile(
    r"\b(employment type|job type|work type|full[- ]time|part[- ]time|contract|permanent|temporary|internship|"
    r"salary|compensation|pay range|base pay|reports to|department|team|location|remote|hybrid|on-?site|"
    r"requisition|job (?:id|number|code)|posted|closing date|start date)\b", re.I)
_JD_EEO_RE = re.compile(
    r"equal (?:employment )?opportunity|eeo\b|affirmative action|reasonable accommodations?|"
    r"diversity (?:and|&) inclusion|does not discriminate|all qualified applicants", re.I)
_JD_YOU_RE = re.compile(r"\b(you will|you(?:'|’)ll|you(?:'|’)re|you have|you are|your|we(?:'|’)re looking|we are looking)\b", re.I)
_JD_CLOSED_RE = re.compile(
    r"(position|job|role|vacancy|posting|requisition|opportunity)\s+(?:has been|is|was|is now|has now been)\s+(?:filled|closed|no longer|cancel+ed|removed|expired)|"
    r"no longer (?:accepting|available|open)|this (?:job|position|posting|vacancy|role) (?:has )?(?:expired|closed|been filled)|"
    r"(?:job|position|posting) (?:not found|does not exist|is not available)|"
    r"currently no (?:open )?(?:positions|vacancies|openings)|no (?:open )?(?:positions|vacancies|openings) (?:at (?:the|this) (?:moment|time)|available|currently)|"
    r"we (?:do not|don(?:'|’)t) have any (?:open )?(?:positions|vacancies|openings)", re.I)


def jd_score(text: str, html_li_count: int = 0) -> tuple[float, list[str]]:
    """How much does `text` read like ONE job description? Returns (score, reasons).
    ~3.0+ is a JD; <2 is not. Multi-language headings are included. Calibrated on live pages
    (see check_page_extract.py): real JDs score 4-9, blog/marketing/'careers' landing pages 0-2.5.
    A page that lists many jobs ("Apply" 6+ times) is penalised -- that is a listing, not a posting."""
    if not text:
        return 0.0, []
    reasons: list[str] = []
    score = 0.0
    n = len(text)
    score += min(n / 1500.0, 2.0)
    reasons.append(f"len={n}")
    heads = {m.group(1).lower() for m in _JD_HEADING_RE.finditer(text)}
    if heads:
        h = min(len(heads), 5)
        score += h * 0.8
        reasons.append(f"headings={len(heads)}")
    applies = len(_JD_APPLY_RE.findall(text))
    if applies:
        score += 1.0 if applies <= 3 else 0.0
        reasons.append(f"apply={applies}")
    if applies >= 6:
        score -= 2.0
        reasons.append("listing-like(apply>=6)")
    metas = {m.group(1).lower() for m in _JD_META_RE.finditer(text)}
    if metas:
        score += min(len(metas), 4) * 0.4
        reasons.append(f"meta={len(metas)}")
    if _JD_EEO_RE.search(text):
        score += 1.0
        reasons.append("eeo")
    you = len(_JD_YOU_RE.findall(text))
    if you >= 3:
        score += 1.0
        reasons.append(f"you={you}")
    bullets = max(html_li_count, len(re.findall(r"(?:^|\s)[•●▪‣⁃\-\*]\s+\S", text)))
    if bullets >= 4:
        score += 1.0
        reasons.append(f"bullets={bullets}")
    if _JD_CLOSED_RE.search(text[:3000]):
        score -= 4.0
        reasons.append("closed/expired")
    return score, reasons


def looks_like_closed_posting(text: str) -> bool:
    return bool(_JD_CLOSED_RE.search(text[:3000]))


JD_MIN_SCORE = 3.0
JD_MIN_CHARS = 200

# Section headings that only a job description really has (not 'benefits' / 'you will' / 'skills', which
# marketing and 'why join us' pages are full of).
_CORE_JD_HEADING_RE = re.compile(
    r"\b(responsibilit(?:y|ies)|requirements?|qualifications?|duties|what you(?:'|\u2019)?ll do|what you will do|"
    r"about the (?:role|job|position)|job description|job summary|position summary|essential functions|"
    r"ideal candidate|aufgaben|anforderungen|missions?|profil)\b", re.I)


def jd_features(text: str, html_li_count: int = 0) -> dict:
    core = {m.group(1).lower() for m in _CORE_JD_HEADING_RE.finditer(text)}
    meta = {m.group(1).lower() for m in _JD_META_RE.finditer(text)}
    score, reasons = jd_score(text, html_li_count)
    return {
        "score": score, "reasons": reasons, "core": len(core), "meta": len(meta),
        "apply": len(_JD_APPLY_RE.findall(text)), "li": html_li_count, "closed": looks_like_closed_posting(text),
    }


def is_job_description(text: str, html_li_count: int = 0) -> bool:
    """The confirmation gate for a fetched detail page. Generic 'sounds like a job ad' signals are NOT enough
    (live calibration: product, 'why join us', help and category pages scored 4-7 just like real JDs), so on top
    of the score a page needs a JD-specific anchor: an Apply call-to-action together with a core JD section /
    >= 2 job-meta labels / a list of >= 10 bullets, or two core JD sections with job-meta labels, or three
    core sections. Closed/expired postings never pass."""
    if len(text) < JD_MIN_CHARS:
        return False
    f = jd_features(text, html_li_count)
    if f["closed"]:
        return False
    if f["apply"] >= 6:  # a listing, not a posting
        return False
    if f["apply"] >= 1 and f["score"] >= 2.5 and (f["core"] >= 1 or f["meta"] >= 2 or f["li"] >= 10):
        return True
    if f["score"] >= JD_MIN_SCORE and ((f["core"] >= 2 and f["meta"] >= 2) or f["core"] >= 3):
        return True
    return False


# ── main-content (boilerplate-free) text ─────────────────────────────────────

_DROP_TAGS = "script,style,noscript,template,svg,canvas,nav,footer,iframe,object,embed,dialog,select,option"
_BOILER_TOKENS = frozenset({
    "cookie", "cookies", "consent", "gdpr", "ccpa", "cmp", "newsletter", "subscribe", "breadcrumb", "breadcrumbs",
    "social", "share", "sharing", "sidebar", "popup", "modal", "offcanvas", "navbar", "nav", "navigation", "menu",
    "footer", "masthead", "skiplink", "livechat", "related", "comments", "pagination", "toolbar", "searchbar",
    "languageswitcher", "lang", "chatbot", "banner",
})
_BOILER_SELECTOR = ",".join(
    f'[class*="{t}"],[id*="{t}"]' for t in sorted(_BOILER_TOKENS) if t not in ("lang", "cmp", "banner")
) + ',[role="navigation"],[role="contentinfo"],[role="dialog"],[aria-modal="true"]'
_META_LABEL_RE = re.compile(
    r"\b(location|salary|compensation|employment|job type|department|remote|hybrid|on-?site|posted|reports? to|"
    r"requisition|job id|category|work type|schedule|closing)\b", re.I)
_TOKEN_SPLIT_RE = re.compile(r"[^a-z0-9]+")

_CONTAINER_SELECTORS = (
    '[itemprop="description"]', '[class*="job-description"]', '[id*="job-description"]', '[class*="jobdescription"]',
    '[id*="jobdescription"]', '[class*="job_description"]', '[class*="posting-description"]', '[class*="job-detail"]',
    '[class*="jobdetail"]', '[class*="vacancy-detail"]', '[class*="job-content"]', '[class*="position-detail"]',
    '[class*="posting"]', "article", "main", '[role="main"]', "#content", "#main", ".content", ".description",
)


def _tokens(*vals: str) -> set[str]:
    out: set[str] = set()
    for v in vals:
        if v:
            out.update(t for t in _TOKEN_SPLIT_RE.split(v.lower()) if t)
    return out


def _clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", html_lib.unescape(s)).strip()


def main_text(html: str, max_len: int = 30_000) -> tuple[str, int]:
    """(text, li_count). The job's own content with page chrome removed.

    Drops script/style/nav/footer/iframes and anything whose class/id carries a boilerplate token
    (cookie, consent, newsletter, share, sidebar, menu, related-jobs, ...). Safeguards so this can
    only ever lose boilerplate, never the job: a flagged node is kept when it holds > 45% of the
    page's text (e.g. <body class="has-menu">, ASP.NET's page-wide <form>) and an <aside> that
    carries job-meta labels (Location / Salary / Employment type...) is kept -- many job pages put
    those facts in a sidebar. Then it picks the best content container (itemprop=description,
    .job-description, article, main, ...) by jd_score, falling back to the whole cleaned body when
    the container holds < 40% of it (so a stray short '.description' can't hide the requirements)."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return "", 0
    body = tree.body
    if body is None:
        return "", 0
    total = len(body.text(deep=True, separator=" ", strip=True)) or 1

    keep_meta: list[str] = []
    for node in tree.css("aside"):
        txt = _clean_ws(node.text(deep=True, separator=" ", strip=True))
        if 0 < len(txt) <= 1500 and len(_META_LABEL_RE.findall(txt)) >= 2:
            keep_meta.append(txt)
    for node in tree.css(_DROP_TAGS):
        node.decompose()
    for node in tree.css("aside"):
        node.decompose()
    for node in tree.css(_BOILER_SELECTOR):
        try:
            attrs = node.attributes
            if node.tag in ("html", "body", "main", "article"):
                continue
            toks = _tokens(attrs.get("class") or "", attrs.get("id") or "")
            if not (toks & _BOILER_TOKENS) and not attrs.get("role") and not attrs.get("aria-modal"):
                continue
            if len(node.text(deep=True, separator=" ", strip=True)) > 0.45 * total:
                continue
            node.decompose()
        except Exception:
            continue

    body = tree.body
    if body is None:
        return "", 0
    body_text = _clean_ws(body.text(deep=True, separator=" ", strip=True))
    best_text, best_score, best_li = body_text, jd_score(body_text)[0], len(body.css("li"))

    for sel in _CONTAINER_SELECTORS:
        try:
            nodes = tree.css(sel)[:4]
        except Exception:
            continue
        for node in nodes:
            txt = _clean_ws(node.text(deep=True, separator=" ", strip=True))
            if len(txt) < 300 or len(txt) < 0.4 * len(body_text):
                continue
            sc = jd_score(txt)[0]
            if sc > best_score + 0.01:
                best_text, best_score, best_li = txt, sc, len(node.css("li"))

    text = best_text
    if keep_meta:
        text = f"{text} {' '.join(keep_meta)}"
    return text[:max_len], best_li


# ── structured sources ───────────────────────────────────────────────────────

def _first_str(d: dict, keys) -> str:
    for k in keys:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            for kk in ("name", "label", "value", "title", "text", "city"):
                vv = v.get(kk)
                if isinstance(vv, str) and vv.strip():
                    return vv.strip()
    return ""


def _stringify_location(v) -> str:
    """Location in all the shapes seen in the wild: str, {name|city|state|country}, [..], {address:{..}}."""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        if isinstance(v.get("address"), dict):
            v = v["address"]
        parts = []
        for k in ("city", "addressLocality", "locality", "name", "label", "state", "region", "addressRegion",
                  "country", "addressCountry", "countryName"):
            x = v.get(k)
            if isinstance(x, dict):
                x = x.get("name")
            if isinstance(x, str) and x.strip() and x.strip() not in parts:
                parts.append(x.strip())
        if parts and ("name" in v or "label" in v) and len(parts) > 1 and v.get("name") in parts:
            # "name" is usually the full display name already; do not double up with city
            return v["name"].strip() if v.get("name") else ", ".join(parts)
        return ", ".join(parts)
    if isinstance(v, list):
        seen: list[str] = []
        for x in v:
            s = _stringify_location(x)
            if s and s not in seen:
                seen.append(s)
        return "; ".join(seen)
    return ""


_TITLE_KEYS = ("title", "jobTitle", "job_title", "positionTitle", "position_title", "postingTitle", "Posting_Title",
               "Job_Opening_Name", "position", "name", "role", "vacancyTitle", "jobName", "job_name", "headline")
_URL_KEYS = ("absolute_url", "absoluteUrl", "applyUrl", "apply_url", "jobUrl", "job_url", "jobPostingUrl", "canonicalUrl",
             "canonical_url", "detailUrl", "detail_url", "careersUrl", "careers_url", "permalink", "url", "link", "href",
             "hostedUrl", "publicUrl", "path")
_LOC_KEYS = ("location", "locations", "jobLocation", "job_location", "office", "offices", "city", "workLocation",
             "primaryLocation", "location_name", "locationName", "Location")
_DESC_KEYS = ("description", "jobDescription", "job_description", "Job_Description", "content", "body", "summary",
              "descriptionHtml", "description_html", "overview", "details", "descriptionPlain")
_DEPT_KEYS = ("department", "Department_Name", "team", "category", "function", "departmentName", "business_unit")
_EVIDENCE_KEYS = re.compile(
    r"^(location|locations|city|office|department|team|employment_?type|job_?type|workplace_?type|remote|is_?remote|"
    r"apply_?url|posted|date_?posted|datePosted|requisition|req_?id|job_?id|salary|"
    r"compensation|seniority|experience|Posting_Title|Job_Opening_Name|Remote_Job|Publish|Date_Opened)$", re.I)
_JOBISH_PARENT_RE = re.compile(r"job|position|opening|vacanc|posting|career|role|requisition|opportunit|offer", re.I)
_URL_PATH_JOBISH_RE = re.compile(r"/(?:jobs?|careers?|positions?|openings?|vacanc(?:y|ies)|postings?|roles?|requisitions?|o|apply|offers?|stellen)/", re.I)


def _job_from_state_item(item: dict, base_url: str, company: str) -> dict | None:
    title = _first_str(item, _TITLE_KEYS)
    if not title or not (3 <= len(title) <= 200) or title.startswith(("http://", "https://")):
        return None
    url = ""
    for k in _URL_KEYS:
        v = item.get(k)
        if isinstance(v, str) and v.strip() and not v.strip().startswith(("#", "javascript:", "mailto:")):
            url = v.strip()
            break
    if not url and item.get("Posting_Title") and item.get("id"):  # Zoho Recruit on a custom domain
        o = urlparse(base_url)
        url = f"{o.scheme}://{o.netloc}/jobs/Careers/{item['id']}"
    if not url:
        return None
    try:
        url = urljoin(base_url, url)
    except ValueError:
        return None
    if urlparse(url).scheme not in ("http", "https"):
        return None
    loc = ""
    for k in _LOC_KEYS:
        if k in item:
            loc = _stringify_location(item[k])
            if loc and k == "city":  # a bare city: add the state / country that sit beside it
                loc = _stringify_location({kk: item.get(kk) for kk in ("city", "state", "country") if isinstance(item.get(kk), (str, dict))})
            if loc:
                break
    if not loc:
        loc = _stringify_location({k: item.get(k) for k in ("city", "state", "country") if isinstance(item.get(k), (str, dict))})
    desc_raw = _first_str(item, _DESC_KEYS)
    desc = _clean_ws(re.sub(r"<[^>]+>", " ", html_lib.unescape(desc_raw))) if desc_raw else ""
    workplace = ""
    rem = item.get("Remote_Job", item.get("remote", item.get("isRemote", item.get("is_remote"))))
    if isinstance(rem, bool):
        workplace = "Remote" if rem else ""
    wp = item.get("workplaceType") or item.get("workplace_type") or item.get("workType")
    if isinstance(wp, str) and wp.strip():
        workplace = wp.strip()
    return {
        "title": title[:500], "url": url, "location": loc, "description": desc,
        "department": _first_str(item, _DEPT_KEYS), "workplace_type": workplace,
        "company": company, "source_ats": "in_house", "clearance": "",
    }


def _walk_job_lists(obj, parent_key: str = "", depth: int = 0, budget: list | None = None):
    """Yield (parent_key, list_of_dicts) for every list of >=1 dicts in the JSON tree."""
    if budget is None:
        budget = [200_000]
    budget[0] -= 1
    if depth > 12 or budget[0] <= 0:
        return
    if isinstance(obj, list):
        dicts = [x for x in obj if isinstance(x, dict)]
        if dicts and len(dicts) >= max(1, len(obj) // 2):
            yield parent_key, dicts
        for x in obj[:2000]:
            yield from _walk_job_lists(x, parent_key, depth + 1, budget)
    elif isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (list, dict)):
                yield from _walk_job_lists(v, str(k), depth + 1, budget)


def _id_lookups(data) -> dict[str, dict]:
    """{"location": {19704: "Amsterdam"}, "department": {...}} from every list of {id, name} records in the JSON (jobs often
    carry only location_id / department_id and ship the tables beside them)."""
    out: dict[str, dict] = {}
    for parent_key, dicts in _walk_job_lists(data):
        if not parent_key or not dicts or not all("id" in d and ("name" in d or "label" in d) and "title" not in d and "url" not in d for d in dicts[:5]):
            continue
        base = re.sub(r"(?:ies|s)$", lambda m: "y" if m.group(0) == "ies" else "", parent_key.lower())
        out.setdefault(base, {}).update({str(d["id"]): _first_str(d, ("name", "label")) for d in dicts if d.get("id") is not None})
    return out


def _resolve_ids(item: dict, lookups: dict) -> dict:
    """Copy of `item` where X_id / Xid keys are resolved to names under X (location_id -> location, department_id -> department)."""
    extra = {}
    for k, v in item.items():
        m = re.fullmatch(r"(.+?)(?:_id|Id)", k)
        if m and v is not None and not isinstance(v, (dict, list)):
            name = lookups.get(m.group(1).lower(), {}).get(str(v))
            if name and m.group(1) not in item:
                extra[m.group(1)] = name
    return {**item, **extra} if extra else item


def _jobs_from_json(data, base_url: str, company: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    lookups = _id_lookups(data)
    for parent_key, dicts in _walk_job_lists(data):
        if lookups:
            dicts = [_resolve_ids(d, lookups) for d in dicts]
        if len(dicts) > 1500:
            continue
        built = [(d, _job_from_state_item(d, base_url, company)) for d in dicts]
        good = [(d, j) for d, j in built if j]
        if not good:
            continue
        evidence = sum(1 for d, _ in good if any(_EVIDENCE_KEYS.match(k) for k in d))
        jobish_url = sum(1 for _, j in good if _URL_PATH_JOBISH_RE.search(urlparse(j["url"]).path + "/"))
        parent_ok = bool(_JOBISH_PARENT_RE.search(parent_key))
        # Menus / footers / blog lists are also arrays of {title, url}: demand real job evidence.
        if not (evidence >= max(1, len(good) // 2) or (parent_ok and jobish_url >= max(1, len(good) // 2))):
            continue
        for _, j in good:
            if j["url"] not in seen:
                seen.add(j["url"])
                out.append(j)
    return out


_SCRIPT_BLOCK_RE = re.compile(r"<script\b([^>]*)>(.*?)</script>", re.I | re.S)
_WINDOW_ASSIGN_RE = re.compile(r"(?:window|self|globalThis)\s*(?:\.\s*([A-Za-z_$][\w$]*)|\[\s*[\"']([^\"']+)[\"']\s*\])\s*=\s*(?=[\[{])")
_VAR_ASSIGN_RE = re.compile(r"\b(?:var|let|const)\s+([A-Za-z_$][\w$]*)\s*=\s*(?=[\[{])")
_INPUT_VALUE_RE = re.compile(r"<input\b[^>]*?\bvalue=\"((?:\[|\{|&#91;|&#123;|&lbrack;|&lbrace;)[^\"]*)\"", re.I)
_DATA_ATTR_RE = re.compile(
    r"(?:\bdata-(?:props|page|react-props|jobs|job-list|positions|state|initial-state|vacancies|openings)"
    # Vue SSR props (Homerun: <job-list v-bind="{&quot;content&quot;:{&quot;vacancies&quot;:[...]}}">), :prop="{...}"
    r"|\bv-bind|\s:[a-z][a-z0-9-]*)=\"([^\"]+)\"", re.I)
_JSON_DECODER = json.JSONDecoder()


def _json_candidates(html: str):
    """Yield every embedded JSON document in the page (parsed)."""
    for m in _SCRIPT_BLOCK_RE.finditer(html):
        attrs, body = m.group(1), m.group(2).strip()
        if not body:
            continue
        if re.search(r"ld\+json", attrs, re.I):
            continue  # JSON-LD has its own path
        if re.search(r"type=[\"']application/(?:json|x-json)|id=[\"'](?:__NEXT_DATA__|__NUXT_DATA__|__NUXT__)", attrs, re.I):
            try:
                yield json.loads(body)
                continue
            except (ValueError, RecursionError):
                pass
        if len(body) < 80:
            continue
        for rx in (_WINDOW_ASSIGN_RE, _VAR_ASSIGN_RE):
            for am in rx.finditer(body):
                try:
                    obj, _ = _JSON_DECODER.raw_decode(body, am.end())  # balanced parse; no first-"});" truncation
                    yield obj
                except (ValueError, RecursionError):
                    continue
    for m in _INPUT_VALUE_RE.finditer(html):
        try:
            yield json.loads(html_lib.unescape(m.group(1)))
        except (ValueError, RecursionError):
            continue
    for m in _DATA_ATTR_RE.finditer(html):
        raw = html_lib.unescape(m.group(1)).strip()
        if raw[:1] in "[{":
            try:
                yield json.loads(raw)
            except (ValueError, RecursionError):
                continue


def extract_state_jobs(html: str, page_url: str, company: str) -> list[dict]:
    """Jobs found in embedded JSON state (see module docstring). Server-rendered SPA shells (Next/Nuxt/
    Gatsby/Remix/Inertia), Wix warmup data, custom `window.__DATA__`, hidden-input JSON (Zoho Recruit)."""
    out: list[dict] = []
    seen: set[str] = set()
    n_docs = 0
    for data in _json_candidates(html):
        n_docs += 1
        if n_docs > 40:
            break
        for j in _jobs_from_json(data, page_url, company):
            if j["url"] not in seen:
                seen.add(j["url"])
                out.append(j)
    return out


def extract_microdata_jobs(html: str, page_url: str, company: str) -> list[dict]:
    """schema.org JobPosting as itemscope/itemprop microdata (the non-JSON-LD syntax)."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return []
    out: list[dict] = []
    for scope in tree.css('[itemscope][itemtype*="JobPosting"]'):
        def prop(name: str) -> str:
            n = scope.css_first(f'[itemprop="{name}"]')
            if n is None:
                return ""
            if n.tag in ("meta",):
                return (n.attributes.get("content") or "").strip()
            if n.tag in ("a", "link") and n.attributes.get("href"):
                return n.attributes["href"].strip()
            return _clean_ws(n.text(deep=True, separator=" ", strip=True))
        title = prop("title")
        if not title:
            continue
        href = ""
        for n in scope.css('[itemprop="url"]'):
            href = n.attributes.get("href") or n.attributes.get("content") or ""
            if href:
                break
        loc_parts = [prop(p) for p in ("addressLocality", "addressRegion", "addressCountry")]
        loc = ", ".join(dict.fromkeys(p for p in loc_parts if p)) or prop("jobLocation")
        try:
            url = urljoin(page_url, href) if href else page_url
        except ValueError:
            continue
        wp = "Remote" if "TELECOMMUTE" in prop("jobLocationType").upper() else ""
        out.append({
            "title": title[:500], "url": url, "location": loc, "description": prop("description"),
            "department": "", "workplace_type": wp, "company": prop("name") or company,
            "source_ats": "in_house", "clearance": "",
        })
    return out


# ── embedded boards: iframes / frames / loader scripts ───────────────────────

_FRAME_JUNK_HOST_RE = re.compile(
    r"(?:^|\.)(googletagmanager|google-analytics|googleapis|gstatic|doubleclick|youtube|youtube-nocookie|youtu|vimeo|"
    r"facebook|fbcdn|twitter|x|instagram|linkedin|tiktok|pinterest|maps\.google|google|recaptcha|hotjar|hubspot|"
    r"hsforms|hs-analytics|calendly|typeform|stripe|paypal|wistia|spotify|soundcloud|addthis|sharethis|disqus|"
    r"intercom|drift|crisp|tawk|zdassets|zendesk|trustpilot|cookiebot|onetrust|usercentrics|trustarc|"
    r"cloudflare|jsdelivr|cdnjs|unpkg|bootstrapcdn|fontawesome|typekit|adobe|clarity|bing|qualtrics|"
    r"surveymonkey|mailchimp|list-manage|eventbrite|zoom|webex|gotowebinar|tableau|powerbi|flickr|"
    r"giphy|tenor|slideshare|scribd|issuu|canva|figma)\.(?:com|net|org|io|tv|be|co)$", re.I)
_FRAME_JOBISH_RE = re.compile(r"job|career|recruit|vacanc|talent|apply|hire|hiring|position|opening|stellen|karriere|emploi|empleo|bewerb", re.I)
_FRAME_ATTRS = ("src", "data-src", "data-lazy-src", "data-url")


def _site(host: str) -> str:
    parts = (host or "").lower().split(".")
    return ".".join(parts[-3:] if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "gov", "ac") else parts[-2:])


def find_job_frames(html: str, page_url: str, limit: int = 3) -> list[str]:
    """Absolute URLs of embedded documents that may hold the real job board, best first: <iframe>/<frame>
    src (and lazy data-src), <embed src>, <object data>, plus CROSS-HOST <script src> loaders on a recruiting-
    looking host. Tracking / video / map / social / widget hosts are skipped. Job-ish URLs sort first; an
    unknown cross-host iframe is still returned (that is how an unrecognised ATS such as app.trinethire.com or
    login.hrwize.com gets read); a same-site non-job iframe (marketing form, pardot...) is not."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return []
    scored: dict[str, int] = {}
    page_site = _site(urlparse(page_url).hostname or "")

    def consider(raw: str, bonus: int, script: bool = False):
        raw = (raw or "").strip()
        if not raw or raw.startswith(("about:", "javascript:", "data:", "#", "mailto:")):
            return
        try:
            full = urljoin(page_url, raw)
            p = urlparse(full)
        except ValueError:
            return
        if p.scheme not in ("http", "https") or not p.hostname:
            return
        host = p.hostname.lower()
        if _FRAME_JUNK_HOST_RE.search(host):
            return
        if re.search(r"\.(?:css|png|jpe?g|gif|svg|webp|ico|woff2?|ttf|mp4|pdf)(?:$|\?)", p.path, re.I) or "gtm" in p.path.lower():
            return
        cross = _site(host) != page_site
        if script and not (cross and _FRAME_JOBISH_RE.search(host)):
            return
        score = bonus + (3 if _FRAME_JOBISH_RE.search(host + p.path) else 0) + (1 if cross else 0)
        if score >= 2:
            scored[full] = max(scored.get(full, -1), score)

    for node in tree.css("iframe,frame"):
        for a in _FRAME_ATTRS:
            consider(node.attributes.get(a) or "", 1)
    for node in tree.css("embed[src]"):
        consider(node.attributes.get("src") or "", 1)
    for node in tree.css("object[data]"):
        consider(node.attributes.get("data") or "", 1)
    for node in tree.css("script[src]"):
        consider(node.attributes.get("src") or "", 1, script=True)
    return [u for u, _ in sorted(scored.items(), key=lambda kv: -kv[1])][:limit]


# ── feeds + WordPress REST ───────────────────────────────────────────────────

def find_feed_links(html: str, page_url: str) -> list[str]:
    """<link rel=alternate type=application/rss+xml|atom+xml> -- job-ish ones only (a site's blog feed
    is not its job feed)."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return []
    out = []
    for node in tree.css('link[rel~="alternate"]'):
        t = (node.attributes.get("type") or "").lower()
        if "rss" not in t and "atom" not in t:
            continue
        href = node.attributes.get("href") or ""
        title = node.attributes.get("title") or ""
        if href and _FRAME_JOBISH_RE.search(href + " " + title):
            try:
                out.append(urljoin(page_url, href))
            except ValueError:
                continue
    return out[:2]


def parse_feed_jobs(xml_text: str, page_url: str, company: str) -> list[dict]:
    """RSS 2.0 / Atom items -> jobs (title, link, description/content)."""
    jobs: list[dict] = []
    for m in re.finditer(r"<(item|entry)\b[^>]*>(.*?)</\1>", xml_text or "", re.I | re.S):
        body = m.group(2)

        def tag(name: str) -> str:
            mm = re.search(rf"<{name}\b[^>]*>(.*?)</{name}>", body, re.I | re.S)
            if not mm:
                return ""
            v = mm.group(1)
            v = re.sub(r"^\s*<!\[CDATA\[(.*?)\]\]>\s*$", r"\1", v, flags=re.S)
            return html_lib.unescape(v).strip()

        title = _clean_ws(re.sub(r"<[^>]+>", " ", tag("title")))
        link = tag("link")
        if not link:
            lm = re.search(r"<link\b[^>]*href=[\"']([^\"']+)", body, re.I)
            link = lm.group(1) if lm else ""
        desc_html = tag("content:encoded") or tag("description") or tag("content") or tag("summary")
        if not title or not link:
            continue
        try:
            url = urljoin(page_url, link.strip())
        except ValueError:
            continue
        jobs.append({
            "title": title[:500], "url": url, "location": "",
            "description": _clean_ws(re.sub(r"<[^>]+>", " ", desc_html)), "department": "", "workplace_type": "",
            "company": company, "source_ats": "in_house", "clearance": "",
        })
    return jobs


_WP_API_LINK_RE = re.compile(r'<link[^>]+rel=["\']https://api\.w\.org/["\'][^>]+href=["\']([^"\']+)', re.I)
_WP_JOB_TYPE_RE = re.compile(r"job|career|position|vacanc|opening|opportunit|posting|recruit|role", re.I)


def wp_api_root(html: str, page_url: str) -> str | None:
    m = _WP_API_LINK_RE.search(html)
    if not m:
        return None
    try:
        return urljoin(page_url, html_lib.unescape(m.group(1)))
    except ValueError:
        return None


def wp_job_endpoints(types_json) -> list[str]:
    """From GET {root}wp/v2/types: rest_base of post types that look like jobs (WP Job Manager's
    job_listing, Jobs/Careers/Positions plugins, custom post types)."""
    out = []
    if isinstance(types_json, dict):
        for key, t in types_json.items():
            if not isinstance(t, dict):
                continue
            hay = f"{key} {t.get('slug', '')} {t.get('name', '')} {t.get('rest_base', '')}"
            if _WP_JOB_TYPE_RE.search(hay) and t.get("rest_base"):
                out.append(str(t["rest_base"]))
    return out[:2]


def parse_wp_posts(items, company: str) -> list[dict]:
    jobs = []
    for it in items if isinstance(items, list) else []:
        if not isinstance(it, dict):
            continue
        t = it.get("title")
        title = _clean_ws(re.sub(r"<[^>]+>", " ", t.get("rendered", ""))) if isinstance(t, dict) else _clean_ws(str(t or ""))
        link = it.get("link")
        if not title or not isinstance(link, str):
            continue
        c = it.get("content")
        desc_html = c.get("rendered", "") if isinstance(c, dict) else ""
        meta = it.get("meta") if isinstance(it.get("meta"), dict) else {}
        loc = ""
        for k in ("_job_location", "_location", "job_location", "location"):
            if isinstance(meta.get(k), str) and meta[k].strip():
                loc = meta[k].strip()
                break
        jobs.append({
            "title": title[:500], "url": link, "location": loc,
            "description": _clean_ws(re.sub(r"<[^>]+>", " ", desc_html)), "department": "", "workplace_type": "",
            "company": company, "source_ats": "in_house", "clearance": "",
        })
    return jobs


# ── the page itself is one posting ───────────────────────────────────────────

_LANDING_TITLE_RE = re.compile(
    r"\b(careers?|join(?:ing)?|opportunit(?:y|ies)|culture|life at|why (?:work|join)|work (?:with|for|at) us|our (?:team|people)|"
    r"openings?|vacancies|recruit(?:ing|ment)|employment|hiring now|benefits|about us|office locations?|"
    r"stellenangebote|karriere|offres? d(?:'|\u2019)emploi|empleo)\b", re.I)
_HIRING_PREFIX_RE = re.compile(r"^(?:we(?:'|\u2019)?re|we are|now|urgently)\s+hiring\s*[:\-\u2013\u2014]?\s*", re.I)


def single_job_page(html: str, page_url: str, company: str) -> dict | None:
    """A page with no listing and no JSON-LD that is itself ONE job description (e.g.
    jamesway.com/careers/supplier-quality-engineer/). Needs: a specific title (<h1>/og:title -- not
    'Careers', 'Join the X team', 'Build your career with ...'; a leading "We're hiring:" is stripped),
    a JD-shaped body (jd_score >= 4.5) that carries >= 2 distinct CORE JD sections (responsibilities /
    requirements / qualifications / duties / about the role ...) -- marketing 'careers' landing pages
    score as high as real JDs on generic signals but almost never have two real JD sections -- and
    it must not be a closed/expired posting or a listing (>= 6 apply buttons)."""
    title = _HIRING_PREFIX_RE.sub("", page_title(html)).strip()
    if is_generic_title(title) or _LANDING_TITLE_RE.search(title) or len(title.split()) > 14:
        return None
    text, li = main_text(html)
    if looks_like_closed_posting(text) or len(text) < 400 or jd_score(text, li)[0] < 4.5:
        return None
    if len({m.group(1).lower() for m in _CORE_JD_HEADING_RE.finditer(text)}) < 2:
        return None
    if len(_JD_APPLY_RE.findall(text)) >= 6:
        return None
    return {
        "title": title[:500], "url": page_url, "location": "", "description": text, "department": "",
        "workplace_type": "", "company": company, "source_ats": "in_house", "clearance": "",
    }


# ── sitemap job URLs (last-resort for JS-rendered boards whose detail pages are still server-rendered) ──

_SITEMAP_LOC_RE = re.compile(r"<loc>\s*(?:<!\[CDATA\[)?\s*([^<\]\s]+)\s*(?:\]\]>)?\s*</loc>", re.I)
_JOB_URL_PATH_RE = re.compile(
    r"/(?:jobs?|careers?|positions?|vacanc(?:y|ies)|openings?|opportunit(?:y|ies)|postings?|stellen|stellenangebote|"
    r"emplois?|offres?|empleos?|requisitions?|roles?)/(?:[^/?#]+/)*[^/?#]*[a-z][^/?#]*-[^/?#]*[a-z][^/?#]*/?$", re.I)
_JOBISH_SITEMAP_NAME_RE = re.compile(r"job|career|vacanc|position|opening|posting|opportunit|stellen|emploi|empleo", re.I)


def parse_sitemap(xml_text: str) -> tuple[list[str], list[str]]:
    """(child_sitemap_urls, page_urls) from a sitemap or sitemap index."""
    locs = _SITEMAP_LOC_RE.findall(xml_text or "")
    if re.search(r"<sitemapindex\b", xml_text or "", re.I):
        return locs, []
    return [], locs


def pick_job_sitemaps(children: list[str], limit: int = 2) -> list[str]:
    return [c for c in children if _JOBISH_SITEMAP_NAME_RE.search(c)][:limit]


def slug_title(url: str) -> str:
    """'/jobs/senior-customer-success-manager-4821' -> 'Senior Customer Success Manager' (a title guess used ONLY
    to decide whether a detail page is worth fetching; the page's own h1 replaces it)."""
    seg = urlparse(url).path.rstrip("/").rsplit("/", 1)[-1]
    seg = re.sub(r"\.(?:html?|php|aspx?)$", "", seg, flags=re.I)
    toks = [t for t in re.split(r"[-_+]+", seg) if t]
    while toks and (re.fullmatch(r"[0-9a-f]{6,}|\d+", toks[-1], re.I) or len(toks[-1]) > 24):
        toks.pop()
    while toks and re.fullmatch(r"\d+", toks[0]):
        toks.pop(0)
    return " ".join(toks).title() if len(toks) >= 2 else ""


def sitemap_job_candidates(urls: list[str], limit: int = 400) -> list[dict]:
    out, seen = [], set()
    for u in urls:
        if u in seen or not _JOB_URL_PATH_RE.search(urlparse(u).path):
            continue
        seen.add(u)
        t = slug_title(u)
        if t:
            out.append({"title": t, "url": u, "_slug_title": True})
        if len(out) >= limit:
            break
    return out


# ── job-link candidates on a listing page (moved here from crawl_ii.py so Crawl I's generic board scraper can share it) ──

# ── pagination ──────────────────────────────────────────────────────────────────────────────────────────────────────
# 2026-10 (explicit user instruction: "it has to get them all"): the old finder matched only an <a> whose WHOLE text was one
# of ~10 English phrases, so a board that paginated with numbered links, an icon-only arrow, "Next →", "Siguiente", a <button
# data-url=...> or "Page 1 of 5" silently returned page 1 only. Everything below reads the page's own markup only (a link,
# data attribute, form or JSON value that is really there); the single URL it may derive is the next value of a page
# parameter the current URL already carries.
_NEXT_WORDS = (
    # English
    r"next(?:\s*(?:page|\d+|set|results?|jobs?|postings?|roles?|openings?|positions?))*|older(?:\s*(?:jobs?|postings?|roles?|results?|posts?))?|"
    r"more(?:\s*(?:jobs?|roles?|postings?|results?|openings?|positions?))?|(?:show|load|view|see|get)\s*more(?:\s*(?:jobs?|roles?|postings?|results?|openings?|positions?))?|"
    r"(?:show|load|view|see)\s*(?:all|next)\s*(?:jobs?|roles?|postings?|results?)?|forward|go\s*to\s*next\s*page|"
    # Spanish / Portuguese / Italian
    r"siguiente(?:s)?(?:\s*p[a\u00e1]gina)?|p[a\u00e1]gina\s*siguiente|ver\s*m[a\u00e1]s|cargar\s*m[a\u00e1]s|mostrar\s*m[a\u00e1]s|"
    r"pr[o\u00f3]xim[ao](?:\s*p[a\u00e1]gina)?|pr[o\u00f3]xima\s*p[a\u00e1]gina|ver\s*mais|carregar\s*mais|mostrar\s*mais|"
    r"avanti|success(?:ivo|iva)|pagina\s*success(?:iva|ivo)|pi[u\u00f9]\s*risultati|carica\s*altri|mostra\s*altri|"
    # French
    r"suivant(?:e)?|page\s*suivante|voir\s*plus|charger\s*plus|afficher\s*plus|plus\s*d.offres|"
    # German / Dutch / Nordic / Slavic / Turkish / Romanian / Asian
    r"weiter|n[a\u00e4]chste(?:\s*seite)?|seite\s*weiter|mehr\s*(?:laden|anzeigen|jobs|stellen)|weitere\s*(?:stellen|jobs|ergebnisse|angebote)|"
    r"volgende(?:\s*pagina)?|meer\s*(?:laden|tonen|vacatures)|n[a\u00e6]ste|neste|n[a\u00e4]sta|seuraava|"
    r"nast[e\u0119]pn\w*|dal[s\u0161][i\u00ed]\w*|n[a\u00e1]sleduj\w*|urm[a\u0103]toarea|sonraki|daha\s*fazla|"
    r"\u0441\u043b\u0435\u0434\u0443\u044e\u0449\w+|\u0434\u0430\u043b\u0435\u0435|\u0435\u0449\u0451|"
    r"\u6b21\u3078|\u6b21\u306e\u30da\u30fc\u30b8|\u4e0b\u4e00\u9875|\u4e0b\u4e00\u9801|\u4e0b\u9875|\ub2e4\uc74c|\ub354\s*\ubcf4\uae30|\u0627\u0644\u062a\u0627\u0644\u064a"
)
_EDGE_JUNK_RE = re.compile(r"^[\W_]+|[\W_]+$")
_RIGHT_ARROWS = "\u203a\u00bb>\u2192\u276f\u2771\u25b6\u25b8\u25ba\u2794\u279c\u27a1\u27e9\u3009\u21d2\u27f6\u2b95\u2192\u21a6\u25b7\u276d\u2759"
NEXT_ARROW_ONLY_RE = re.compile(r"^[\s" + re.escape(_RIGHT_ARROWS) + r"]+$")
_NEXT_WORDS_RE = re.compile(r"(?:" + _NEXT_WORDS + r")", re.I)
_PREV_WORDS_RE = re.compile(
    r"^(?:prev(?:ious)?(?:\s*page)?|back|newer|anterior|p[a\u00e1]gina\s*anterior|pr[e\u00e9]c[e\u00e9]dent(?:e)?|zur[u\u00fc]ck|vorherige(?:\s*seite)?|vorige|"
    r"precedente|\u4e0a\u4e00\u9875|\u524d\u3078)$", re.I)


def normalize_nav_text(text: str) -> str:
    """Lower-cased, whitespace-collapsed, with arrows / punctuation trimmed off both ends ("Next →" -> "next")."""
    return _EDGE_JUNK_RE.sub("", re.sub(r"\s+", " ", text or "").strip()).lower().strip()


def is_next_label(text: str) -> bool:
    """True for a pagination 'next' label in any of the supported phrasings / languages, with or without an arrow."""
    raw = re.sub(r"\s+", " ", text or "").strip()
    if not raw or len(raw) > 40:
        return False
    if NEXT_ARROW_ONLY_RE.match(raw):
        return True
    norm = normalize_nav_text(raw)
    return bool(norm) and bool(_NEXT_WORDS_RE.fullmatch(norm))


# Kept under the old name: the exact-phrase test other modules import.
class _NextPageTextMatcher:
    @staticmethod
    def match(text):
        return True if is_next_label(text) else None


NEXT_PAGE_TEXT_RE = _NextPageTextMatcher()

JOB_HREF_RE = re.compile(
    r"/(?:job|jobs|career|careers|position|positions|opening|openings|"
    r"vacanc(?:y|ies)|opportunit(?:y|ies)|role|roles|"
    # 2026-10: Factorial (/job_posting/<slug>-<id>), Gusto (/postings/), requisitions, DACH / FR / ES vocabulary
    r"job[_-]?postings?|postings?|requisitions?|vacatures?|stellen(?:angebote)?|offres?|empleos?|ofertas?)/[\w\-./%]+", re.I)
GENERIC_LINK_TEXT_RE = re.compile(
    r"^(apply( now| here| today| online)?|view( job| details| position| role| opening)?|read more|learn more|details?|"
    r"more( info(rmation)?)?|see (details|role|more)|open|more|angebot ansehen|voir l.offre|ver oferta|bekijk vacature)$", re.I)

NAV_TEXT_BLOCKLIST_RE = re.compile(
    r"^(home|about( us)?|contact( us)?|blog|news|press|privacy( policy)?|terms"
    r"( (of|and) (service|conditions|use))?|cookies?( policy)?|"
    r"sign[\s-]?in|log[\s-]?in|sign[\s-]?up|register|faq|help|support|our team|"
    r"careers?|open positions?|current openings?|view all( jobs)?|see all|"
    r"learn more|read more|apply( now)?|search|filter|next|previous|"
    r"load more|back to (search|jobs|careers)|share this job)$", re.I)


_NEXT_TOKEN_RE = re.compile(
    r"^(?:[a-z0-9]*[-_]+)*(?:next|nextpage|load[-_]?more|show[-_]?more|view[-_]?more|see[-_]?more|chevron[-_]?right|"
    r"angle[-_]?right|arrow[-_]?right|caret[-_]?right|right[-_]?arrow|right[-_]?chevron|forward)(?:[-_]+[a-z0-9]*)*$")
_NEXT_TOKEN_BLOCK = ("__next", "nextjs", "next-js", "next-auth", "nextdoor", "next-line", "nextline")
_DISABLED_TOKEN_RE = re.compile(r"(?:^|[-_ ])(?:disabled|inactive|unavailable|hidden)(?:$|[-_ ])", re.I)
_PAGINATION_CONTAINER_RE = re.compile(r"(?:^|[-_ ])(?:pagination|paginator|pager|paging|page[-_]?nav\w*|pages)(?:$|[-_ ])", re.I)
_PAGE_PARAMS = ("page", "pg", "p", "pagenumber", "page_number", "pageindex", "page_index", "paged", "pagenum", "page_num", "currentpage")
_URL_DATA_ATTRS = ("href", "data-href", "data-url", "data-next", "data-next-url", "data-next-page", "data-next-page-url",
                   "data-load-more-url", "data-more-url", "data-endpoint", "data-path", "data-page-url", "data-link", "hx-get",
                   "formaction", "data-action-url", "data-src-url")
_ONCLICK_URL_RE = re.compile(
    r"(?:location(?:\.href)?|window\.open|window\.location(?:\.href)?|location\.assign|location\.replace)\s*[=(]\s*['\"]([^'\"]+)['\"]")
_JSON_NEXT_KEY_RE = re.compile(
    r"[\"'](?:next_?page_?url|next_?page_?link|next_?url|next_?link|load_?more_?url|more_?url|nextHref|next_?href)[\"']\s*:\s*[\"']([^\"']+)[\"']", re.I)
_PAGE_X_OF_Y_RE = re.compile(r"\bpage\s+(\d{1,4})\s*(?:of|/)\s*(\d{1,4})\b|\b(\d{1,4})\s*(?:of|/)\s*(\d{1,4})\s+pages\b", re.I)


def _site_key(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return ""
    parts = host.split(".")
    if len(parts) >= 3 and parts[-2] in ("co", "com", "org", "net", "gov", "ac") and len(parts[-1]) == 2:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:]) if len(parts) >= 2 else host


def _classes(node) -> list[str]:
    return [t.lower() for t in (node.attributes.get("class") or "").split() if t]


def _is_disabled(node) -> bool:
    a = node.attributes
    if "disabled" in a or (a.get("aria-disabled") or "").lower() == "true" or (a.get("tabindex") == "-1" and not a.get("href")):
        return True
    own = " ".join(_classes(node))
    par = node.parent
    par_cls = " ".join(_classes(par)) if par is not None and par.tag in ("li", "span", "div") else ""
    return bool(_DISABLED_TOKEN_RE.search(own) or _DISABLED_TOKEN_RE.search(par_cls))


def _node_label_texts(node) -> list[str]:
    out = [node.text(deep=True, separator=" ").strip()]
    a = node.attributes
    for k in ("aria-label", "title", "data-label", "value", "alt"):
        if a.get(k):
            out.append(a[k])
    try:
        for img in node.css("img[alt]"):
            out.append(img.attributes.get("alt") or "")
        for t in node.css("svg title, svg desc"):
            out.append(t.text(deep=True).strip())
    except Exception:
        pass
    return [t for t in out if t]


def _has_next_token(node) -> bool:
    """A 'next' class / id / data-testid token on the control itself (short text only: a whole job card that is one <a> must not
    count), or a next-looking icon class (fa-chevron-right, icon-arrow-right) INSIDE a control that has no text of its own."""
    text = re.sub(r"\s+", " ", node.text(deep=True, separator=" ").strip())
    own = _classes(node)
    for k in ("id", "data-testid", "data-test", "data-qa", "data-cy", "data-action", "data-role", "name"):
        v = (node.attributes.get(k) or "").lower().strip()
        if v:
            own.append(v)
    if len(text) <= 25 and len(text.split()) <= 3:
        for t in own:
            if not t.startswith(_NEXT_TOKEN_BLOCK) and _NEXT_TOKEN_RE.match(t):
                return True
    if not re.search(r"[^\W_]", text):          # icon-only control
        try:
            for ic in node.css("i, svg, span"):
                for t in _classes(ic):
                    if not t.startswith(_NEXT_TOKEN_BLOCK) and _NEXT_TOKEN_RE.match(t):
                        return True
        except Exception:
            pass
    return False


def _node_urls(node, page_url: str) -> list[str]:
    """Every URL this control carries in its own markup: href, data-* / hx-get attributes, onclick navigation, GET-form."""
    out = []
    for k in _URL_DATA_ATTRS:
        v = (node.attributes.get(k) or "").strip()
        if v and not v.startswith(("#", "javascript:", "mailto:", "tel:")) and " " not in v:
            out.append(v)
    oc = node.attributes.get("onclick") or ""
    m = _ONCLICK_URL_RE.search(oc)
    if m:
        out.append(m.group(1))
    # <button> inside a GET form whose page-number input we can read: next = that value + 1
    anc = node.parent
    for _ in range(6):
        if anc is None:
            break
        if anc.tag == "form" and (anc.attributes.get("method") or "get").lower() == "get":
            action = anc.attributes.get("action") or page_url
            q = {}
            for inp in anc.css("input[name]"):
                nm, val = inp.attributes.get("name") or "", inp.attributes.get("value") or ""
                if (inp.attributes.get("type") or "").lower() in ("submit", "button", "checkbox", "radio", "file"):
                    continue
                q[nm] = val
            for nm in list(q):
                if nm.lower() in _PAGE_PARAMS and q[nm].isdigit():
                    q[nm] = str(int(q[nm]) + 1)
                    out.append(action + ("&" if "?" in action else "?") + "&".join(f"{k}={v}" for k, v in q.items()))
                    break
            break
        anc = anc.parent
    resolved = []
    for u in out:
        try:
            resolved.append(urljoin(page_url, u))
        except ValueError:
            continue
    return resolved


def _page_param_url(url: str, delta: int = 1) -> str | None:
    """The same URL with its OWN page-number parameter (?page=2, /page/2/) moved by delta, or None if it carries none."""
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    q = parse_qsl(parsed.query, keep_blank_values=True)
    for i, (k, v) in enumerate(q):
        if k.lower() in _PAGE_PARAMS and v.isdigit():
            q[i] = (k, str(max(int(v) + delta, 0)))
            return parsed._replace(query=urlencode(q)).geturl()
    m = re.search(r"(/(?:page|pages|p|seite|pagina)/)(\d{1,4})(/?)$", parsed.path, re.I)
    if m:
        return parsed._replace(path=parsed.path[:m.start()] + m.group(1) + str(int(m.group(2)) + delta) + m.group(3)).geturl()
    return None


def _page_number_of(url: str) -> int | None:
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    for k, v in parse_qsl(parsed.query):
        if k.lower() in _PAGE_PARAMS and v.isdigit():
            return int(v)
    m = re.search(r"/(?:page|pages|p|seite|pagina)/(\d{1,4})/?$", parsed.path, re.I)
    return int(m.group(1)) if m else None


def _in_pagination_container(node) -> bool:
    anc = node
    for _ in range(6):
        if anc is None:
            return False
        blob = " ".join(_classes(anc) + [anc.attributes.get("id") or "", anc.attributes.get("aria-label") or "",
                                          anc.attributes.get("role") or "", anc.attributes.get("data-testid") or ""])
        if anc.tag == "nav" and re.search(r"pag|page", blob, re.I):
            return True
        if _PAGINATION_CONTAINER_RE.search(blob):
            return True
        anc = anc.parent
    return False


def find_pagination_urls(html: str, page_url: str) -> list[str]:
    """Every URL that leads to another page of THIS listing, best-signal first, from the page's own markup:

      1. <link rel="next"> / <a rel="next">;
      2. any control (a / button / role=button / li / icon) whose text, aria-label, title, alt, svg <title> or class token says
         'next' / 'load more' / an arrow, in English or 10+ other languages (not disabled), reading its URL from href,
         data-href / data-url / data-next*, hx-get, onclick navigation or a GET form's page input;
      3. numbered pagination ("1 2 3 ... 10"): every numbered link of the same listing, the pages after the current one first;
      4. next-page URL keys in embedded JSON (next_page_url, nextLink, ...);
      5. the current URL's own page parameter + 1 ("?page=2" -> "?page=3", "/page/2/" -> "/page/3/") when the page shows
         pagination but offers no resolvable link.
    Same-site only; never the page itself. Callers follow all of them (visited-set + 'adds nothing new' end the walk)."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return []
    site = _site_key(page_url)
    found: list[str] = []
    seen: set[str] = set()

    def add(u: str, trusted: bool = False):
        try:
            u = urljoin(page_url, u).split("#")[0]
            pu = urlparse(u)
        except ValueError:
            return
        if pu.scheme not in ("http", "https") or u == page_url.split("#")[0] or u in seen:
            return
        if not trusted and _site_key(u) != site:
            return
        seen.add(u)
        found.append(u)

    # 1. standards signals
    for ln in tree.css('link[rel~="next"]'):
        if ln.attributes.get("href"):
            add(ln.attributes["href"], trusted=True)
    for a in tree.css('a[rel~="next"], a[rel~="Next"]'):
        for u in _node_urls(a, page_url):
            add(u, trusted=True)

    # hrefs that carry a real text title on this page are job / content links; an icon-only arrow pointing at one of them
    # ("Bangalore, India  ->") opens that job, it does not page the list
    titled_hrefs: set[str] = set()
    for a in tree.css("a[href]"):
        t_ = re.sub(r"\s+", " ", a.text(deep=True, separator=" ").strip())
        if len(t_.split()) >= 2 and not is_next_label(t_):
            try:
                titled_hrefs.add(urljoin(page_url, a.attributes.get("href") or "").split("#")[0])
            except ValueError:
                pass

    # 2. next / load-more controls
    for node in tree.css("a, button, [role=button], [role=link], li, span, div, input[type=button], input[type=submit]"):
        tag = node.tag
        if tag in ("span", "div", "li"):
            a_ = node.attributes
            if not (a_.get("role") in ("button", "link") or a_.get("onclick") or any(k in a_ for k in _URL_DATA_ATTRS)):
                continue
        if _is_disabled(node):
            continue
        labels = _node_label_texts(node)
        is_prev = any(_PREV_WORDS_RE.match(normalize_nav_text(t)) for t in labels) or "prev" in (node.attributes.get("rel") or "").lower()
        if is_prev:
            continue
        labelled = any(is_next_label(t) for t in labels)
        if labelled or (_has_next_token(node) and (tag in ("a", "button") or _in_pagination_container(node))):
            for u in _node_urls(node, page_url):
                if not labelled and u.split("#")[0] in titled_hrefs:
                    continue
                add(u)

    # list-item wrappers: <li class="next"><a href=...>
    for li in tree.css("li, span, div"):
        if _is_disabled(li) or any(_classes(li) and t.startswith(_NEXT_TOKEN_BLOCK) for t in _classes(li)):
            continue
        if any(_NEXT_TOKEN_RE.match(t) and not t.startswith(_NEXT_TOKEN_BLOCK) for t in _classes(li)) and (
                li.tag == "li" or _in_pagination_container(li)):
            for a in li.css("a[href]"):
                if not _is_disabled(a) and not any(_PREV_WORDS_RE.match(normalize_nav_text(t)) for t in _node_label_texts(a)):
                    for u in _node_urls(a, page_url):
                        add(u)

    # 3. numbered pagination
    numbered: dict[int, str] = {}
    current = _page_number_of(page_url)
    for a in tree.css("a[href]"):
        href = (a.attributes.get("href") or "").strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        txt = re.sub(r"\s+", " ", a.text(deep=True, separator=" ").strip())
        aria = (a.attributes.get("aria-label") or a.attributes.get("title") or "")
        m = re.fullmatch(r"\d{1,4}", txt) or re.fullmatch(r"(?:go to |goto |page |p\.? ?|seite |p\u00e1gina |pagina )\s*(\d{1,4})", aria.strip(), re.I)
        if not m:
            continue
        n = int(m.group(1) if m.groups() else m.group(0))
        try:
            full = urljoin(page_url, href).split("#")[0]
        except ValueError:
            continue
        if not (_in_pagination_container(a) or _page_number_of(full) is not None or _page_param_url(full, 0)):
            continue
        if (a.attributes.get("aria-current") or "").lower() in ("page", "true") or any(
                t in ("active", "current", "selected", "is-active", "is-current") for t in _classes(a) + (_classes(a.parent) if a.parent is not None else [])):
            current = n
            continue
        numbered.setdefault(n, full)
    if numbered:
        after = sorted(n for n in numbered if current is None or n > current)
        before = sorted((n for n in numbered if current is not None and n < current), reverse=True)
        for n in after:
            add(numbered[n])
        for n in before:
            add(numbered[n])

    # 4. embedded JSON
    for sc in tree.css("script"):
        body = sc.text(deep=True) or ""
        if len(body) > 2_000_000 or not body.strip().startswith(("{", "[", "window.", "var ", "self.")) and (sc.attributes.get("type") or "") not in ("application/json", "application/ld+json"):
            continue
        for m in _JSON_NEXT_KEY_RE.finditer(body):
            add(m.group(1).replace("\\/", "/").replace("\\u0026", "&").replace("&amp;", "&"))

    # 5. the URL's own page parameter + 1, only when the page shows pagination but offered nothing resolvable
    if not found and pagination_control_present(html):
        nxt = _page_param_url(page_url, 1)
        if nxt:
            add(nxt)
    return found


def pagination_control_present(html: str) -> bool:
    """True when the page shows ANY sign of more pages (a next / load-more control that is not disabled, a numbered pager,
    'Page 1 of N' with N > 1). Used to flag boards whose extra pages could not be resolved to a URL (JavaScript-only)."""
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return False
    for node in tree.css("a, button, [role=button], input[type=button], input[type=submit]"):
        if _is_disabled(node):
            continue
        labels = _node_label_texts(node)
        if any(_PREV_WORDS_RE.match(normalize_nav_text(t)) for t in labels):
            continue
        if any(is_next_label(t) for t in labels):
            return True
        if _has_next_token(node) and (node.tag in ("a", "button") or _in_pagination_container(node)):
            if node.tag == "a" and any(len(re.sub(r"\s+", " ", o.text(deep=True, separator=" ").strip()).split()) >= 2
                                       and o.attributes.get("href") == node.attributes.get("href") and o is not node
                                       for o in tree.css("a[href]")):
                continue
            return True
    for a in tree.css("a[href]"):
        txt = a.text(deep=True, separator=" ").strip()
        if re.fullmatch(r"\d{1,4}", txt) and txt != "1" and _in_pagination_container(a):
            return True
    m = _PAGE_X_OF_Y_RE.search(tree.body.text(deep=True, separator=" ") if tree.body is not None else "")
    if m:
        total = int(m.group(2) or m.group(4) or 0)
        if total > 1:
            return True
    return False


def find_next_page_url(html: str, page_url: str) -> str | None:
    """First pagination URL on the page, or None. Kept for callers that follow a single chain; the crawls use
    find_pagination_urls and walk all of them."""
    urls = find_pagination_urls(html, page_url)
    return urls[0] if urls else None


HEADING_SELECTOR = "h1,h2,h3,h4,h5,h6,[class*=title],[class*=heading],[class*=job-name],[class*=jobname],strong"


def card_title(a) -> str:
    """Best title for a job link whose own text is not a title: a heading / title element INSIDE the link (cards that
    wrap everything in one <a>: Freshteam, onlyfy, ...), else the first one in the nearest enclosing card (title in a
    sibling element, link text just "Apply now": Factorial, ...). "" when nothing plausible (2-12 words)."""
    def pick(node) -> str:
        try:
            for h in node.css(HEADING_SELECTOR):
                t = re.sub(r"\s+", " ", h.text(deep=True, separator=" ", strip=True))
                if t and 2 <= len(t.split()) <= 12 and not NAV_TEXT_BLOCKLIST_RE.match(t) and not GENERIC_LINK_TEXT_RE.match(t):
                    return t
        except Exception:
            pass
        return ""
    t = pick(a)
    if t:
        return t
    node = a.parent
    for _ in range(4):
        if node is None or node.tag in ("body", "html", "main"):
            break
        t = pick(node)
        if t:
            return t
        node = node.parent
    return ""


def find_job_link_candidates(html: str, page_url: str) -> list[dict]:
    try:
        tree = LexborHTMLParser(html)
    except Exception:
        return []

    candidates: dict[str, dict] = {}
    fingerprint_groups: dict[tuple, list] = {}

    for a in tree.css("a[href]"):
        href = a.attributes.get("href") or ""
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue
        text = a.text(deep=True, separator=" ").strip()
        text = re.sub(r"\s+", " ", text)
        word_count = len(text.split())
        slug_guess = False
        # A real job title reads like a short phrase, not a single nav word
        # and not a whole sentence/paragraph — 2-12 words in practice.
        if not text or word_count < 2 or word_count > 12 or NAV_TEXT_BLOCKLIST_RE.match(text.strip()) \
                or GENERIC_LINK_TEXT_RE.match(text.strip()):
            # 2026-10: but a link that is clearly a JOB url (/jobs/<id>/<slug>, /job_posting/<slug>) is kept when its
            # text is a button label ("Apply now"), a whole card (> 12 words), or empty: the title then comes from a
            # heading in the card, else from the URL slug. Class-level fix for card layouts (Freshteam, Factorial,
            # onlyfy ...) where the link text is never the title.
            if not JOB_HREF_RE.search(href):
                continue
            card = card_title(a)
            if card:
                text = card
            else:
                try:
                    guess = slug_title(urljoin(page_url, href))
                except Exception:
                    guess = ""
                if not guess:
                    continue
                text, slug_guess = guess, True

        # 2026-09: a real crash killed a whole crawl_ii.py shard —
        # urljoin/urlparse can raise ValueError on a malformed href (seen
        # live: an href attribute value of 'sjm code="11" ', almost
        # certainly a parser mis-grab from broken/non-HTML markup on some
        # page, not a real URL at all). One bad <a> tag on one page must
        # not take down the whole batch — skip just that link.
        try:
            full_url = urljoin(page_url, href)
            parsed = urlparse(full_url)
        except ValueError:
            continue
        if parsed.scheme not in ("http", "https"):
            continue

        if JOB_HREF_RE.search(href):
            cand = {"title": text[:300], "url": full_url}
            if slug_guess:
                cand["_slug_title"] = True
            candidates.setdefault(full_url, cand)
            continue

        parent = a.parent
        if parent is None:
            continue
        fp = (parent.tag, parent.attributes.get("class") or "")
        fingerprint_groups.setdefault(fp, []).append((full_url, text))

    for (tag, cls), items in fingerprint_groups.items():
        # A shared class is a strong repetition signal (3+ siblings); with
        # no class to key on at all, require a bigger group (5+) since
        # tag-only repetition (e.g. every <li> on the page) is much weaker.
        min_group = 3 if cls else 5
        seen_in_group = set()
        unique_items = []
        for url, text in items:
            if url in seen_in_group:
                continue
            seen_in_group.add(url)
            unique_items.append((url, text))
        if len(unique_items) < min_group:
            continue
        for url, text in unique_items:
            candidates.setdefault(url, {"title": text[:300], "url": url})

    return list(candidates.values())


