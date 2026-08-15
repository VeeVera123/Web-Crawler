"""Robust multi-fallback application-question scraper for public ATS job forms.

Strategy (per URL):
  1. ATS-specific public/semipublic endpoints and application URL variants.
  2. Embedded JSON state / JSON-LD / script payload extraction.
  3. Static DOM form extraction, including labels/fieldset/options.
  4. Iframe discovery and recursive parsing.
  5. Playwright-rendered DOM + embedded state + iframe traversal.

The scraper never submits an application and never attempts authentication bypass.
It only reads publicly accessible job/application pages.
"""

from __future__ import annotations

import copy
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 15
DEFAULT_BROWSER_TIMEOUT_MS = 25_000

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
)

# 24 existing platforms + a few high-value additions.
ATS_PATTERNS = [
    ("greenhouse", ("greenhouse.io", "boards.greenhouse.io")),
    ("lever", ("lever.co",)),
    ("ashby", ("ashbyhq.com",)),
    ("workable", ("workable.com",)),
    ("bamboohr", ("bamboohr.com",)),
    ("icims", ("icims.com",)),
    ("workday", ("myworkdayjobs.com",)),
    ("recruitee", ("recruitee.com",)),
    ("smartrecruiters", ("smartrecruiters.com",)),
    ("taleo", ("taleo.net",)),
    ("oracle_cloud_hcm", ("oraclecloud.com",)),
    ("brassring", ("brassring.com",)),
    ("teamtailor", ("teamtailor.com",)),
    ("successfactors", ("successfactors.com", "successfactors.eu")),
    ("breezyhr", ("breezy.hr",)),
    ("applytojob", ("applytojob.com",)),
    ("hrmdirect", ("hrmdirect.com",)),
    ("softgarden", ("softgarden.io", "softgarden.de")),
    ("zoho", ("zohorecruit.com",)),
    ("ycombinator", ("workatastartup.com",)),
    ("personio", ("personio.de", "personio.com", "jobs.personio.com")),
    ("joincom", ("join.com",)),
    ("paylocity", ("paylocity.com",)),
    ("rippling", ("rippling.com",)),
    # Additional coverage.
    ("pinpoint", ("pinpointhq.com",)),
    ("comeet", ("comeet.com", "comeet.co")),
    ("dayforce", ("dayforcehcm.com", "dayforce.com")),
    ("jobvite", ("jobvite.com",)),
]

QUESTION_CONTAINER_KEYS = {
    "questions", "question", "screeningquestions", "screening_questions",
    "applicationquestions", "application_questions", "customquestions",
    "custom_questions", "fields", "formfields", "form_fields", "customfields",
    "custom_fields", "applicationform", "application_form", "prescreenquestions",
    "pre_screen_questions", "questionnaire", "questionnaires", "jobapplicationform",
}

QUESTION_LABEL_KEYS = ("label", "question", "prompt", "title", "text", "name", "description")
QUESTION_TYPE_KEYS = ("type", "inputType", "input_type", "fieldType", "field_type", "controlType", "control_type")
OPTION_KEYS = ("options", "choices", "answer_options", "answerOptions", "values", "choicesList")

NOISE_NAMES = re.compile(r"(?:csrf|token|nonce|captcha|recaptcha|tracking|analytics|utm)", re.I)
NOISE_TYPES = {"hidden", "submit", "button", "reset", "image"}

@dataclass
class ScrapeResult:
    questions: list[dict]
    ats: str
    method: str
    attempts: list[str]
    final_url: str
    error: str = ""


class GlobalATSScraper:
    """Public application-question extractor with layered fallbacks."""

    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        browser_timeout_ms: int = DEFAULT_BROWSER_TIMEOUT_MS,
        use_browser: bool = True,
        max_iframes: int = 8,
    ):
        self.timeout = timeout
        self.browser_timeout_ms = browser_timeout_ms
        self.use_browser = use_browser
        self.max_iframes = max_iframes
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json, text/html, application/xhtml+xml, application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        })

    # ----------------------------- public API -----------------------------

    def detect_ats(self, url: str) -> str:
        domain = urlparse(url).netloc.lower().split(":", 1)[0]
        for name, domains in ATS_PATTERNS:
            if any(d in domain for d in domains):
                return name
        return "unknown"

    def get_application_questions(self, url: str, return_meta: bool = False):
        result = self.scrape(url)
        return result if return_meta else result.questions

    def scrape(self, url: str) -> ScrapeResult:
        ats = self.detect_ats(url)
        attempts: list[str] = []
        best_questions: list[dict] = []
        best_method = "none"
        best_url = url

        # Level 1: ATS-specific endpoint/page variants. Never stop merely
        # because an endpoint returned two generic fields; another endpoint
        # or the rendered form may contain custom screening questions.
        for candidate, label in self._candidate_urls(url, ats):
            attempts.append(f"L1|{label}|{candidate}")
            try:
                response = self._get(candidate)
                if not response:
                    continue
                questions = self._clean(self._extract_from_response(response, ats))
                if len(questions) > len(best_questions):
                    best_questions = questions
                    best_method = label
                    best_url = response.url
                # Greenhouse's public questions endpoint is authoritative for
                # the public job-board form; no browser is needed when it works.
                if ats == "greenhouse" and label == "greenhouse_public_job_api" and questions:
                    return ScrapeResult(questions, ats, label, attempts, response.url)
            except Exception as exc:
                log.debug("L1 %s failed for %s: %s", label, url, exc)

        # Level 2 + 3: static HTML state, JSON-LD, DOM and iframe traversal.
        try:
            response = self._get(url)
            if response:
                attempts.append(f"L2/L3|static|{response.url}")
                soup = BeautifulSoup(response.text, "html.parser")
                questions = []
                questions.extend(self._extract_json_state(soup))
                questions.extend(self._extract_json_ld(soup))
                questions.extend(self._extract_dom(soup, base_url=response.url))
                questions.extend(self._extract_iframes_static(soup, response.url, attempts))
                questions = self._clean(questions)
                if len(questions) > len(best_questions):
                    best_questions = questions
                    best_method = "static_json_dom"
                    best_url = response.url
        except Exception as exc:
            log.debug("Static extraction failed for %s: %s", url, exc)

        # Level 4: browser-rendered page. For non-Greenhouse ATSs this is
        # deliberately attempted even when static parsing found some fields,
        # because React/Vue/SPA forms commonly inject screening questions only
        # after hydration. We keep whichever result is richer.
        if self.use_browser and ats != "greenhouse":
            try:
                attempts.append("L4|playwright")
                questions, final_url = self._playwright_extract(url)
                questions = self._clean(questions)
                if len(questions) > len(best_questions):
                    best_questions = questions
                    best_method = "playwright_dom_state"
                    best_url = final_url
            except Exception as exc:
                log.debug("Playwright extraction failed for %s: %s", url, exc)

        if best_questions:
            return ScrapeResult(best_questions, ats, best_method, attempts, best_url)
        return ScrapeResult([], ats, "none", attempts, url, "No application questions extracted")

    # ----------------------------- URL strategy -----------------------------

    def _candidate_urls(self, url: str, ats: str) -> Iterable[tuple[str, str]]:
        seen = set()
        def add(u: str, label: str):
            if u and u not in seen:
                seen.add(u)
                yield u, label

        # Original URL first because it may already be the application endpoint.
        yield from add(url, "ats_original")

        path = urlparse(url).path.rstrip("/")
        lower = path.lower()

        # Common application route variants.
        suffixes = {
            "lever": ["/apply"],
            "ashby": ["/application", "/apply"],
            "recruitee": ["/application", "/apply"],
            "workable": ["/apply"],
            "breezyhr": ["/apply"],
            "teamtailor": ["/apply"],
            "greenhouse": ["/apply"],
            "personio": ["/apply"],
            "pinpoint": ["/apply"],
        }.get(ats, ["/apply"])
        if not any(lower.endswith(s) for s in suffixes):
            for suffix in suffixes:
                yield from add(url.rstrip("/") + suffix, f"{ats}_apply_variant")

        if ats == "greenhouse":
            m = re.search(r"boards\.greenhouse\.io/([^/]+)/jobs/(\d+)", url, re.I)
            if m:
                board, job_id = m.groups()
                yield from add(
                    f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs/{job_id}?questions=true",
                    "greenhouse_public_job_api",
                )
                yield from add(
                    f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs/{job_id}",
                    "greenhouse_public_job_api_no_questions",
                )

        if ats == "icims":
            sep = "&" if "?" in url else "?"
            yield from add(url + sep + "mode=apply&in_iframe=1", "icims_iframe_apply")
            yield from add(url + sep + "in_iframe=1", "icims_iframe")

        if ats == "successfactors":
            # Common RMK application route; static page may redirect to the real form.
            for q in ("?jobApplication=true", "?mode=apply", "/apply"):
                yield from add(url + q, "successfactors_variant")

        if ats == "oracle_cloud_hcm":
            for q in ("?mode=apply", "/apply"):
                yield from add(url + q, "oracle_apply_variant")

        if ats == "paylocity":
            yield from add(url + ("&" if "?" in url else "?") + "jobid=", "paylocity_variant")

    # ----------------------------- HTTP extraction -----------------------------

    def _get(self, url: str) -> requests.Response | None:
        try:
            r = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            if r.status_code in (401, 403, 429):
                return None
            r.raise_for_status()
            return r
        except requests.RequestException:
            return None

    def _extract_from_response(self, response: requests.Response, ats: str) -> list[dict]:
        ctype = (response.headers.get("content-type") or "").lower()
        text = response.text
        if "json" in ctype or text.lstrip().startswith(("{", "[")):
            try:
                data = response.json()
                return self._recursive_question_search(data)
            except Exception:
                pass
        soup = BeautifulSoup(text, "html.parser")
        out = []
        out.extend(self._extract_json_state(soup))
        out.extend(self._extract_json_ld(soup))
        out.extend(self._extract_dom(soup, response.url))
        return out

    def _looks_like_application(self, html: str) -> bool:
        s = html.lower()
        signals = [
            "application form", "apply now", "cover letter", "resume", "cv upload",
            "screening question", "work authorization", "linkedin profile", "first name",
        ]
        return sum(x in s for x in signals) >= 2

    # ----------------------------- JSON/state extraction -----------------------------

    def _extract_json_state(self, soup: BeautifulSoup) -> list[dict]:
        found: list[dict] = []
        scripts = soup.find_all("script")
        for script in scripts:
            raw = script.string or script.get_text() or ""
            if not raw.strip():
                continue
            sid = (script.get("id") or "").lower()
            stype = (script.get("type") or "").lower()

            if sid in {"__next_data__", "__nuxt__", "__data__", "application-state", "app-state"}:
                try:
                    found.extend(self._recursive_question_search(json.loads(raw)))
                except Exception:
                    pass
                continue

            if "ld+json" in stype:
                continue

            # Parse full JSON script blocks where possible.
            if raw.lstrip().startswith(("{", "[")):
                try:
                    found.extend(self._recursive_question_search(json.loads(raw)))
                    continue
                except Exception:
                    pass

            # Common JS assignments: window.__INITIAL_STATE__ = {...};
            if any(k.lower() in raw.lower() for k in ("initial_state", "initialstate", "pagedata", "appstate", "applicationform", "questions")):
                for obj in self._extract_balanced_json_objects(raw):
                    try:
                        found.extend(self._recursive_question_search(json.loads(obj)))
                    except Exception:
                        continue
        return found

    def _extract_json_ld(self, soup: BeautifulSoup) -> list[dict]:
        found = []
        for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
            raw = script.string or script.get_text() or ""
            try:
                data = json.loads(raw)
            except Exception:
                continue
            # JSON-LD normally contains job metadata, but some ATSs embed form data nearby.
            found.extend(self._recursive_question_search(data))
        return found

    def _recursive_question_search(self, data: Any, depth: int = 0) -> list[dict]:
        if depth > 30:
            return []
        found: list[dict] = []
        if isinstance(data, dict):
            normalized_keys = {str(k).lower().replace("-", "_"): k for k in data}
            is_questionish = (
                any(k in normalized_keys for k in QUESTION_LABEL_KEYS)
                and any(k.lower() in normalized_keys for k in QUESTION_TYPE_KEYS)
            )
            if is_questionish:
                label = self._first_value(data, QUESTION_LABEL_KEYS)
                if isinstance(label, str) and self._valid_label(label):
                    found.append(self._question_from_mapping(data, label))

            for key, value in data.items():
                key_norm = str(key).lower().replace("-", "_").replace(" ", "")
                if key_norm in {x.replace("_", "") for x in QUESTION_CONTAINER_KEYS} or isinstance(value, (dict, list)):
                    found.extend(self._recursive_question_search(value, depth + 1))
        elif isinstance(data, list):
            for item in data:
                found.extend(self._recursive_question_search(item, depth + 1))
        return found

    @staticmethod
    def _first_value(data: dict, keys: Iterable[str]):
        for k in keys:
            for actual, value in data.items():
                if str(actual).lower() == k.lower() and value not in (None, ""):
                    return value
        return None

    def _question_from_mapping(self, data: dict, label: str) -> dict:
        typ = self._first_value(data, QUESTION_TYPE_KEYS) or "text"
        options = self._first_value(data, OPTION_KEYS) or []
        if isinstance(options, dict):
            options = list(options.values())
        normalized_options = []
        for opt in options if isinstance(options, list) else []:
            if isinstance(opt, dict):
                value = opt.get("label") or opt.get("title") or opt.get("text") or opt.get("name") or opt.get("value")
            else:
                value = opt
            if value not in (None, ""):
                normalized_options.append(str(value).strip())
        required = self._first_value(data, ("required", "isRequired", "is_required", "mandatory"))
        return {
            "label": self._clean_label(label),
            "type": str(typ),
            "required": bool(required) if not isinstance(required, str) else required.lower() in {"true", "1", "yes", "required"},
            "options": normalized_options,
        }

    def _extract_balanced_json_objects(self, text: str) -> list[str]:
        out = []
        for start, ch in enumerate(text):
            if ch not in "[{":
                continue
            depth = 0
            in_string = False
            escaped = False
            quote = ""
            for i in range(start, len(text)):
                c = text[i]
                if in_string:
                    if escaped:
                        escaped = False
                    elif c == "\\":
                        escaped = True
                    elif c == quote:
                        in_string = False
                    continue
                if c in "'\"":
                    in_string = True
                    quote = c
                    continue
                if c in "[{":
                    depth += 1
                elif c in "]}":
                    depth -= 1
                    if depth == 0:
                        out.append(text[start:i + 1])
                        break
        return out[:20]

    # ----------------------------- DOM extraction -----------------------------

    def _extract_dom(self, soup: BeautifulSoup, base_url: str = "") -> list[dict]:
        questions = []
        seen_controls = set()
        for element in soup.find_all(["input", "textarea", "select", "button"]):
            tag = element.name
            typ = (element.get("type") or ("textarea" if tag == "textarea" else tag)).lower()
            name = element.get("name") or ""
            element_id = element.get("id") or ""
            if typ in NOISE_TYPES or NOISE_NAMES.search(name):
                continue
            # Buttons only count when they are actually form controls, not navigation.
            if tag == "button" and typ not in {"submit", "button"}:
                continue
            key = (name, element_id, typ)
            if key in seen_controls:
                continue
            seen_controls.add(key)

            label = self._find_label(element, soup)
            if not label:
                label = element.get("aria-label") or element.get("placeholder") or name
            if not label:
                continue
            label = self._clean_label(label)
            if not self._valid_label(label):
                continue

            required = (
                element.has_attr("required")
                or str(element.get("aria-required", "")).lower() == "true"
                or "required" in str(element.get("class", "")).lower()
                or "*" in label
            )
            options = []
            if tag == "select":
                for opt in element.find_all("option"):
                    txt = opt.get_text(" ", strip=True)
                    if txt and txt.lower() not in {"select...", "select", "choose...", "choose"}:
                        options.append(txt)
            elif typ in {"radio", "checkbox"}:
                # Gather sibling controls with the same name.
                if name:
                    for sib in soup.find_all("input", attrs={"name": name}):
                        val = sib.get("value")
                        if val:
                            options.append(str(val))

            questions.append({
                "label": label,
                "type": typ,
                "required": bool(required),
                "options": list(dict.fromkeys(options)),
            })
        return questions

    def _find_label(self, element, soup: BeautifulSoup) -> str:
        element_id = element.get("id")
        if element_id:
            label = soup.find("label", attrs={"for": element_id})
            if label:
                return label.get_text(" ", strip=True)
        parent = element.find_parent("label")
        if parent:
            clone = copy.copy(parent)
            for child in clone.find_all(["input", "textarea", "select", "button"]):
                child.decompose()
            return clone.get_text(" ", strip=True)
        # Fieldset/legend is particularly useful for radio groups.
        fieldset = element.find_parent("fieldset")
        if fieldset:
            legend = fieldset.find("legend")
            if legend:
                return legend.get_text(" ", strip=True)
        # Search nearby semantic containers.
        for parent in element.parents:
            if getattr(parent, "name", None) not in {"div", "section", "li", "fieldset"}:
                continue
            txt = parent.get_text(" ", strip=True)
            if 3 <= len(txt) <= 300:
                # Prefer the nearest short text rather than an entire page container.
                return txt
            if len(list(parent.parents)) > 6:
                break
        return ""

    def _extract_iframes_static(self, soup: BeautifulSoup, base_url: str, attempts: list[str]) -> list[dict]:
        found = []
        for iframe in soup.find_all("iframe")[: self.max_iframes]:
            src = iframe.get("src") or iframe.get("data-src")
            if not src:
                continue
            iframe_url = urljoin(base_url, src)
            attempts.append(f"L3|iframe|{iframe_url}")
            try:
                r = self._get(iframe_url)
                if not r:
                    continue
                found.extend(self._extract_from_response(r, self.detect_ats(iframe_url)))
            except Exception:
                continue
        return found

    # ----------------------------- Playwright -----------------------------

    def _playwright_extract(self, url: str) -> tuple[list[dict], str]:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=USER_AGENT,
                locale="en-US",
                viewport={"width": 1440, "height": 1100},
            )
            page = context.new_page()
            captured_json = []

            def capture_response(response):
                try:
                    ctype = (response.headers.get("content-type") or "").lower()
                    target = response.url.lower()
                    if "json" not in ctype and not any(k in target for k in ("question", "application", "form", "screening", "prescreen")):
                        return
                    if response.status >= 400:
                        return
                    body = response.json()
                    if isinstance(body, (dict, list)):
                        captured_json.append(body)
                except Exception:
                    pass

            page.on("response", capture_response)
            page.goto(url, wait_until="domcontentloaded", timeout=self.browser_timeout_ms)
            try:
                page.wait_for_load_state("networkidle", timeout=7_000)
            except Exception:
                pass
            # Give React/Vue forms a short stabilization period.
            page.wait_for_timeout(1200)

            found = []
            visited_states = set()

            def collect_current_page():
                html = page.content()
                soup = BeautifulSoup(html, "html.parser")
                found.extend(self._extract_json_state(soup))
                found.extend(self._extract_json_ld(soup))
                found.extend(self._extract_dom(soup, page.url))
                for frame in page.frames:
                    if frame == page.main_frame:
                        continue
                    try:
                        frame_html = frame.content()
                        frame_soup = BeautifulSoup(frame_html, "html.parser")
                        found.extend(self._extract_json_state(frame_soup))
                        found.extend(self._extract_dom(frame_soup, frame.url))
                    except Exception:
                        continue

            # Some ATSs expose the job page first and only reveal the form after
            # an Apply button is clicked. This is navigation only; we never submit.
            def click_apply_if_present():
                patterns = re.compile(r"^\s*(apply(?: now)?|apply for this job|start application)\s*$", re.I)
                for locator in (
                    page.get_by_role("link", name=patterns),
                    page.get_by_role("button", name=patterns),
                ):
                    try:
                        if locator.count() and locator.first.is_visible():
                            locator.first.click(timeout=2500)
                            page.wait_for_timeout(900)
                            return True
                    except Exception:
                        continue
                return False

            click_apply_if_present()

            # Collect every visible step in multi-page application forms.
            # We only click controls labelled Next/Continue/Proceed; Submit/Apply
            # is deliberately excluded so the scraper cannot submit an application.
            for _ in range(8):
                state = (page.url, page.locator("form").count(), page.locator("input,textarea,select").count())
                if state in visited_states:
                    break
                visited_states.add(state)
                collect_current_page()

                next_clicked = False
                next_patterns = re.compile(
                    r"^\s*(next|continue|save and continue|proceed|next step|continue application)\s*$",
                    re.I,
                )
                for locator in (
                    page.get_by_role("button", name=next_patterns),
                    page.get_by_role("link", name=next_patterns),
                    page.locator("input[type='button'], input[type='submit']"),
                ):
                    try:
                        count = locator.count()
                        for i in range(min(count, 5)):
                            item = locator.nth(i)
                            if not item.is_visible():
                                continue
                            label = (item.inner_text() if item.evaluate("el => el.tagName") != "INPUT" else item.get_attribute("value")) or ""
                            if not next_patterns.match(label):
                                continue
                            item.click(timeout=2500)
                            page.wait_for_timeout(800)
                            next_clicked = True
                            break
                        if next_clicked:
                            break
                    except Exception:
                        continue
                if not next_clicked:
                    break

            collect_current_page()
            for payload in captured_json:
                try:
                    found.extend(self._recursive_question_search(payload))
                except Exception:
                    continue
            final_url = page.url
            context.close()
            browser.close()
            return found, final_url

    # ----------------------------- cleaning -----------------------------

    @staticmethod
    def _clean_label(label: str) -> str:
        label = re.sub(r"\s+", " ", str(label)).strip()
        label = re.sub(r"\s*\*\s*$", "", label).strip()
        return label

    @staticmethod
    def _valid_label(label: str) -> bool:
        if not label or len(label) < 2 or len(label) > 1000:
            return False
        low = label.lower()
        noise = {
            "submit", "apply", "next", "back", "continue", "cancel", "search",
            "upload", "choose file", "select", "menu", "close",
        }
        return low not in noise

    def _clean(self, questions: Iterable[dict]) -> list[dict]:
        out = []
        seen = set()
        for q in questions:
            if not isinstance(q, dict):
                continue
            label = self._clean_label(q.get("label", ""))
            if not self._valid_label(label):
                continue
            key = re.sub(r"\W+", " ", label.lower()).strip()
            if key in seen:
                continue
            seen.add(key)
            options = q.get("options") or []
            if not isinstance(options, list):
                options = []
            out.append({
                "label": label,
                "type": str(q.get("type") or "text"),
                "required": bool(q.get("required", False)),
                "options": list(dict.fromkeys(str(x).strip() for x in options if str(x).strip())),
            })
        return out

    @staticmethod
    def _is_good_result(questions: list[dict]) -> bool:
        return len(questions) >= 2


def enrich_jobs_with_application_questions(
    jobs: list[dict],
    scraper: GlobalATSScraper | None = None,
    max_workers: int = 8,
    only_unsure: bool = True,
) -> list[dict]:
    """Attach application questions to jobs, optionally only role-uncertain jobs.

    A job is considered unsure when `role_classification == 'unsure'`,
    `role_status == 'unsure'`, or `keyword_classification == 'unsure'`.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    scraper = scraper or GlobalATSScraper()

    def should_scrape(job: dict) -> bool:
        if not job.get("url"):
            return False
        if not only_unsure:
            return True
        return any(job.get(k) == "unsure" for k in (
            "role_classification", "role_status", "keyword_classification"
        ))

    targets = [j for j in jobs if should_scrape(j)]
    if not targets:
        return jobs

    def one(job):
        result = scraper.scrape(job["url"])
        return job, result

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(one, job) for job in targets]
        for future in futures:
            try:
                job, result = future.result()
                job["application_questions"] = result.questions
                job["application_question_count"] = len(result.questions)
                job["application_question_status"] = "found" if result.questions else "not_found"
                job["application_question_ats"] = result.ats
                job["application_question_method"] = result.method
                job["application_question_attempts"] = result.attempts
            except Exception as exc:
                log.exception("Application question enrichment failed: %s", exc)
    return jobs


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    scraper = GlobalATSScraper(use_browser=not args.no_browser)
    result = scraper.scrape(args.url)
    print(json.dumps({
        "ats": result.ats,
        "method": result.method,
        "count": len(result.questions),
        "questions": result.questions,
        "attempts": result.attempts,
        "final_url": result.final_url,
        "error": result.error,
    }, indent=2))
