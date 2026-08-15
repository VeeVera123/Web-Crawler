"""
Global ATS Application Question Scraper

Multi-fallback application-form extraction engine.

Fallback order:

1. ATS-specific public endpoints
2. ATS-specific application URL variants
3. Embedded JSON state
4. JSON-LD
5. Static DOM
6. iframe DOM
7. Playwright-rendered DOM
8. Playwright Apply-button navigation
9. Playwright multi-step application traversal

This module NEVER submits an application.
"""

from __future__ import annotations

import copy
import json
import logging
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)


class GlobalATSApplicationScraper:

    ATS_DOMAINS = {
        "greenhouse": ("greenhouse.io", "greenhouse.com"),
        "lever": ("lever.co",),
        "ashby": ("ashbyhq.com",),
        "workable": ("workable.com",),
        "bamboohr": ("bamboohr.com",),
        "icims": ("icims.com",),
        "workday": ("myworkdayjobs.com", "workday.com"),
        "recruitee": ("recruitee.com",),
        "smartrecruiters": ("smartrecruiters.com",),
        "taleo": ("taleo.net",),
        "oracle_cloud_hcm": ("oraclecloud.com",),
        "brassring": ("brassring.com",),
        "teamtailor": ("teamtailor.com",),
        "successfactors": ("successfactors.com", "successfactors.eu"),
        "breezyhr": ("breezy.hr",),
        "applytojob": ("applytojob.com",),
        "hrmdirect": ("hrmdirect.com",),
        "softgarden": ("softgarden.io", "softgarden.de"),
        "zoho": ("zohorecruit.com",),
        "ycombinator": ("workatastartup.com",),
        "personio": ("personio.de", "personio.com"),
        "joincom": ("join.com",),
        "paylocity": ("paylocity.com",),
        "rippling": ("rippling.com",),

        # Additional ATSs
        "pinpoint": ("pinpointhq.com",),
        "comeet": ("comeet.com",),
        "dayforce": ("dayforce.com",),
        "jobvite": ("jobvite.com",),
    }

    SKIP_INPUT_TYPES = {
        "hidden",
        "submit",
        "button",
        "file",
        "image",
        "reset",
    }

    NEXT_TEXT = re.compile(
        r"^(next|continue|proceed|save\s*&?\s*continue|"
        r"continue\s*application|next\s*step)$",
        re.I,
    )

    SUBMIT_TEXT = re.compile(
        r"(submit application|submit|apply now|send application|"
        r"complete application|finish application)",
        re.I,
    )

    def __init__(
        self,
        timeout: int = 15,
        browser_timeout: int = 30,
        max_steps: int = 8,
    ):
        self.timeout = timeout
        self.browser_timeout = browser_timeout
        self.max_steps = max_steps

        self.session = requests.Session()

        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        })

    # ============================================================
    # PUBLIC API
    # ============================================================

    def get_application_questions(self, url: str) -> dict[str, Any]:
        """
        Return a structured result.

        {
            "questions": [...],
            "ats": "greenhouse",
            "method": "...",
            "attempts": [...]
        }
        """

        result = {
            "questions": [],
            "ats": self.detect_ats(url),
            "method": "",
            "attempts": [],
        }

        if not url:
            result["method"] = "invalid_url"
            return result

        # --------------------------------------------------------
        # LEVEL 1 — ATS-SPECIFIC ENDPOINTS
        # --------------------------------------------------------

        try:
            result["attempts"].append("ats_specific")

            questions = self._ats_specific(url, result["ats"])

            if questions:
                result["questions"] = self._deduplicate_questions(questions)
                result["method"] = "ats_specific"
                return result

        except Exception as exc:
            log.debug("ATS-specific extraction failed: %s", exc)

        # --------------------------------------------------------
        # LEVEL 2 — APPLICATION URL VARIANTS
        # --------------------------------------------------------

        try:
            result["attempts"].append("application_url_variants")

            for candidate in self._application_url_variants(
                url,
                result["ats"],
            ):
                try:
                    questions = self._static_page(candidate)

                    if len(questions) >= 1:
                        result["questions"] = self._deduplicate_questions(
                            questions
                        )
                        result["method"] = "application_url"
                        return result

                except Exception as exc:
                    log.debug(
                        "Application URL failed %s: %s",
                        candidate,
                        exc,
                    )

        except Exception as exc:
            log.debug("Application URL fallback failed: %s", exc)

        # --------------------------------------------------------
        # LEVEL 3 — EMBEDDED JSON / JSON-LD / DOM
        # --------------------------------------------------------

        try:
            result["attempts"].append("static_html")

            html = self._get(url)

            if html:
                soup = BeautifulSoup(html, "html.parser")

                questions = []

                # JSON state
                questions.extend(
                    self._extract_json_state(soup)
                )

                # JSON-LD
                questions.extend(
                    self._extract_jsonld(soup)
                )

                # DOM
                questions.extend(
                    self._extract_form_elements(soup)
                )

                # iframe
                questions.extend(
                    self._extract_iframes(url, soup)
                )

                questions = self._deduplicate_questions(questions)

                if questions:
                    result["questions"] = questions
                    result["method"] = "static_html"
                    return result

        except Exception as exc:
            log.debug("Static HTML extraction failed: %s", exc)

        # --------------------------------------------------------
        # LEVEL 4 — PLAYWRIGHT
        # --------------------------------------------------------

        try:
            result["attempts"].append("playwright")

            questions = self._playwright_extract(url)

            if questions:
                result["questions"] = self._deduplicate_questions(
                    questions
                )
                result["method"] = "playwright"
                return result

        except Exception as exc:
            log.debug("Playwright extraction failed: %s", exc)

        # --------------------------------------------------------
        # NOTHING FOUND
        # --------------------------------------------------------

        result["method"] = "none"

        return result

    # ============================================================
    # ATS DETECTION
    # ============================================================

    def detect_ats(self, url: str) -> str:
        domain = urlparse(url).netloc.lower()

        for ats, domains in self.ATS_DOMAINS.items():
            for marker in domains:
                if marker in domain:
                    return ats

        return "unknown"

    # ============================================================
    # LEVEL 1 — ATS SPECIFIC
    # ============================================================

    def _ats_specific(
        self,
        url: str,
        ats: str,
    ) -> list[dict]:
        if ats == "greenhouse":
            return self._greenhouse(url)

        if ats == "ashby":
            return self._ashby(url)

        return []

    def _greenhouse(self, url: str) -> list[dict]:
        match = re.search(
            r"(?:boards|job-boards)\.greenhouse\.io/"
            r"([^/]+)/jobs/(\d+)",
            url,
            re.I,
        )

        if not match:
            return []

        board, job_id = match.groups()

        api_url = (
            f"https://boards-api.greenhouse.io/v1/boards/"
            f"{board}/jobs/{job_id}?questions=true"
        )

        response = self.session.get(
            api_url,
            timeout=self.timeout,
        )

        if response.status_code != 200:
            return []

        data = response.json()

        questions = []

        for q in data.get("questions", []):

            label = (
                q.get("label")
                or q.get("text")
                or q.get("name")
            )

            if not label:
                continue

            options = []

            for field in q.get("fields", []) or []:
                if isinstance(field, dict):
                    value = (
                        field.get("label")
                        or field.get("name")
                        or field.get("value")
                    )

                    if value:
                        options.append(str(value))

            questions.append({
                "label": str(label).strip(),
                "type": q.get("type", "text"),
                "required": bool(q.get("required")),
                "options": options,
                "source": "greenhouse_api",
            })

        return questions

    def _ashby(self, url: str) -> list[dict]:
        match = re.search(
            r"ashbyhq\.com/([^/]+)/"
            r"([a-f0-9-]+)",
            url,
            re.I,
        )

        if not match:
            return []

        slug, job_id = match.groups()

        candidates = [
            f"https://api.ashbyhq.com/posting-api/"
            f"posting/{slug}/{job_id}",
            f"https://api.ashbyhq.com/posting-api/"
            f"posting/{slug}/{job_id}/",
        ]

        for api_url in candidates:

            try:
                response = self.session.get(
                    api_url,
                    timeout=self.timeout,
                )

                if response.status_code != 200:
                    continue

                data = response.json()

                questions = self._recursive_question_search(
                    data
                )

                if questions:
                    return questions

            except Exception:
                continue

        return []

    # ============================================================
    # APPLICATION URL VARIANTS
    # ============================================================

    def _application_url_variants(
        self,
        url: str,
        ats: str,
    ) -> list[str]:

        base = url.rstrip("/")

        variants = []

        suffixes = {
            "lever": ["/apply"],
            "ashby": ["/application"],
            "recruitee": ["/application"],
            "icims": [
                "?mode=apply&in_iframe=1",
                "?mode=apply",
            ],
            "workable": ["/apply"],
            "teamtailor": ["/apply"],
            "personio": ["/apply"],
            "breezyhr": ["/apply"],
        }

        for suffix in suffixes.get(ats, []):
            if suffix.startswith("?"):
                candidate = base + suffix
            else:
                candidate = base + suffix

            if candidate != url:
                variants.append(candidate)

        return list(dict.fromkeys(variants))

    # ============================================================
    # HTTP
    # ============================================================

    def _get(self, url: str) -> str:
        response = self.session.get(
            url,
            timeout=self.timeout,
            allow_redirects=True,
        )

        if response.status_code >= 400:
            return ""

        return response.text

    # ============================================================
    # JSON STATE
    # ============================================================

    def _extract_json_state(
        self,
        soup: BeautifulSoup,
    ) -> list[dict]:

        questions = []

        # Next.js
        next_data = soup.find(
            "script",
            id="__NEXT_DATA__",
        )

        if next_data:
            text = next_data.string or next_data.get_text()

            try:
                data = json.loads(text)
                questions.extend(
                    self._recursive_question_search(data)
                )
            except Exception:
                pass

        # Nuxt
        for script_id in (
            "__NUXT_DATA__",
            "__NUXT__",
        ):
            script = soup.find(
                "script",
                id=script_id,
            )

            if script:
                try:
                    data = json.loads(
                        script.string or script.get_text()
                    )

                    questions.extend(
                        self._recursive_question_search(data)
                    )

                except Exception:
                    pass

        # Generic JSON scripts
        for script in soup.find_all("script"):

            text = script.string or script.get_text()

            if not text:
                continue

            if not any(
                keyword in text.lower()
                for keyword in (
                    "question",
                    "applicationform",
                    "formfields",
                    "customfields",
                    "screening",
                )
            ):
                continue

            # JSON script
            try:
                data = json.loads(text)

                questions.extend(
                    self._recursive_question_search(data)
                )

                continue

            except Exception:
                pass

            # Balanced JSON objects inside JavaScript
            for candidate in self._extract_balanced_json(text):

                try:
                    data = json.loads(candidate)

                    questions.extend(
                        self._recursive_question_search(data)
                    )

                except Exception:
                    continue

        return questions

    def _extract_balanced_json(
        self,
        text: str,
    ) -> list[str]:

        results = []

        for opening in ("{", "["):

            stack = []

            start = None
            quote = False
            escape = False

            for index, char in enumerate(text):

                if quote:

                    if escape:
                        escape = False
                    elif char == "\\":
                        escape = True
                    elif char == '"':
                        quote = False

                    continue

                if char == '"':
                    quote = True
                    continue

                if char == opening:
                    if not stack:
                        start = index

                    stack.append(char)

                elif char == ("}" if opening == "{" else "]"):

                    if stack:
                        stack.pop()

                    if not stack and start is not None:

                        candidate = text[start:index + 1]

                        if (
                            len(candidate) > 30
                            and (
                                "question" in candidate.lower()
                                or "field" in candidate.lower()
                                or "application" in candidate.lower()
                            )
                        ):
                            results.append(candidate)

                        start = None

        return results[:100]

    # ============================================================
    # RECURSIVE QUESTION FINDER
    # ============================================================

    def _recursive_question_search(
        self,
        data: Any,
    ) -> list[dict]:

        found = []

        if isinstance(data, dict):

            label = (
                data.get("label")
                or data.get("question")
                or data.get("title")
                or data.get("text")
                or data.get("prompt")
                or data.get("placeholder")
                or data.get("name")
            )

            field_type = (
                data.get("type")
                or data.get("inputType")
                or data.get("fieldType")
                or data.get("component")
            )

            if (
                isinstance(label, str)
                and label.strip()
                and (
                    field_type
                    or any(
                        key in data
                        for key in (
                            "required",
                            "options",
                            "choices",
                            "answers",
                        )
                    )
                )
            ):

                options = (
                    data.get("options")
                    or data.get("choices")
                    or data.get("answers")
                    or []
                )

                if isinstance(options, dict):
                    options = list(options.values())

                clean_options = []

                if isinstance(options, list):

                    for option in options:

                        if isinstance(option, dict):
                            value = (
                                option.get("label")
                                or option.get("name")
                                or option.get("text")
                                or option.get("value")
                            )
                        else:
                            value = option

                        if value:
                            clean_options.append(str(value))

                found.append({
                    "label": label.strip(),
                    "type": field_type or "text",
                    "required": bool(
                        data.get("required")
                        or data.get("isRequired")
                    ),
                    "options": clean_options,
                    "source": "json_state",
                })

            for value in data.values():
                found.extend(
                    self._recursive_question_search(value)
                )

        elif isinstance(data, list):

            for item in data:
                found.extend(
                    self._recursive_question_search(item)
                )

        return found

    # ============================================================
    # JSON-LD
    # ============================================================

    def _extract_jsonld(
        self,
        soup: BeautifulSoup,
    ) -> list[dict]:

        questions = []

        for script in soup.find_all(
            "script",
            attrs={"type": "application/ld+json"},
        ):

            try:
                data = json.loads(
                    script.string or script.get_text()
                )

                questions.extend(
                    self._recursive_question_search(data)
                )

            except Exception:
                continue

        return questions

    # ============================================================
    # DOM
    # ============================================================

    def _extract_form_elements(
        self,
        soup: BeautifulSoup,
    ) -> list[dict]:

        questions = []

        grouped = {}

        elements = soup.find_all(
            ["input", "textarea", "select"]
        )

        for element in elements:

            element_type = (
                element.name
                if element.name != "input"
                else element.get(
                    "type",
                    "text",
                ).lower()
            )

            if element_type in self.SKIP_INPUT_TYPES:
                continue

            name = element.get("name", "").strip()

            if not name:
                name = element.get("id", "").strip()

            if not name:
                name = (
                    element.get(
                        "aria-label",
                        "",
                    ).strip()
                )

            label = self._find_label(
                soup,
                element,
            )

            if not label:
                continue

            required = (
                element.has_attr("required")
                or element.get("aria-required") == "true"
                or "*" in label
            )

            label = re.sub(
                r"\s*\*\s*$",
                "",
                label,
            ).strip()

            options = []

            if element.name == "select":

                for option in element.find_all("option"):

                    text = option.get_text(
                        " ",
                        strip=True,
                    )

                    if (
                        text
                        and text.lower()
                        not in (
                            "select...",
                            "select",
                            "choose...",
                            "choose",
                        )
                    ):
                        options.append(text)

            elif element_type in (
                "radio",
                "checkbox",
            ):

                group_key = name or label

                grouped.setdefault(
                    group_key,
                    {
                        "label": label,
                        "type": element_type,
                        "required": required,
                        "options": [],
                        "source": "dom",
                    },
                )

                value = (
                    element.get("value")
                    or element.get("aria-label")
                )

                if value:
                    grouped[group_key][
                        "options"
                    ].append(value)

                continue

            questions.append({
                "label": label,
                "type": element_type,
                "required": bool(required),
                "options": options,
                "source": "dom",
            })

        questions.extend(grouped.values())

        # fieldset / legend questions
        for fieldset in soup.find_all("fieldset"):

            legend = fieldset.find("legend")

            if not legend:
                continue

            label = legend.get_text(
                " ",
                strip=True,
            )

            if not label:
                continue

            inner = fieldset.find_all(
                ["input", "textarea", "select"]
            )

            if not inner:
                continue

            options = []

            for element in inner:

                value = (
                    element.get("value")
                    or element.get("aria-label")
                )

                if value:
                    options.append(value)

            questions.append({
                "label": label,
                "type": "fieldset",
                "required": any(
                    element.has_attr("required")
                    for element in inner
                ),
                "options": list(
                    dict.fromkeys(options)
                ),
                "source": "fieldset",
            })

        return questions

    def _find_label(
        self,
        soup: BeautifulSoup,
        element,
    ) -> str:

        element_id = element.get("id")

        if element_id:

            label = soup.find(
                "label",
                attrs={"for": element_id},
            )

            if label:
                return label.get_text(
                    " ",
                    strip=True,
                )

        parent_label = element.find_parent(
            "label"
        )

        if parent_label:

            clone = copy.copy(parent_label)

            for child in clone.find_all(
                ["input", "textarea", "select"]
            ):
                child.decompose()

            text = clone.get_text(
                " ",
                strip=True,
            )

            if text:
                return text

        aria = element.get("aria-label")

        if aria:
            return aria.strip()

        placeholder = element.get(
            "placeholder"
        )

        if placeholder:
            return placeholder.strip()

        # Nearby semantic elements
        parent = element.parent

        if parent:

            for sibling in (
                parent.find_previous_sibling(),
                element.find_previous_sibling(),
            ):

                if sibling and getattr(
                    sibling,
                    "name",
                    None,
                ) in (
                    "label",
                    "div",
                    "span",
                    "p",
                    "h3",
                    "h4",
                    "legend",
                ):

                    text = sibling.get_text(
                        " ",
                        strip=True,
                    )

                    if text:
                        return text

        return ""

    # ============================================================
    # IFRAME
    # ============================================================

    def _extract_iframes(
        self,
        base_url: str,
        soup: BeautifulSoup,
    ) -> list[dict]:

        questions = []

        for iframe in soup.find_all("iframe"):

            src = (
                iframe.get("src")
                or iframe.get("data-src")
            )

            if not src:
                continue

            iframe_url = urljoin(
                base_url,
                src,
            )

            try:

                html = self._get(iframe_url)

                if not html:
                    continue

                iframe_soup = BeautifulSoup(
                    html,
                    "html.parser",
                )

                questions.extend(
                    self._extract_json_state(
                        iframe_soup
                    )
                )

                questions.extend(
                    self._extract_form_elements(
                        iframe_soup
                    )
                )

            except Exception:
                continue

        return questions

    # ============================================================
    # PLAYWRIGHT
    # ============================================================

    def _playwright_extract(
        self,
        url: str,
    ) -> list[dict]:

        try:
            from playwright.sync_api import (
                sync_playwright,
            )
        except ImportError:

            log.warning(
                "Playwright is not installed; "
                "browser fallback unavailable."
            )

            return []

        questions = []

        with sync_playwright() as p:

            browser = p.chromium.launch(
                headless=True
            )

            context = browser.new_context(
                user_agent=self.session.headers[
                    "User-Agent"
                ],
                locale="en-US",
            )

            page = context.new_page()

            responses = []

            def capture_response(response):

                try:

                    content_type = (
                        response.headers.get(
                            "content-type",
                            "",
                        ).lower()
                    )

                    if (
                        "json" in content_type
                        or "graphql" in response.url.lower()
                    ):
                        if response.ok:
                            responses.append(response)

                except Exception:
                    pass

            page.on(
                "response",
                capture_response,
            )

            try:

                page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=self.browser_timeout * 1000,
                )

                try:
                    page.wait_for_load_state(
                        "networkidle",
                        timeout=10000,
                    )
                except Exception:
                    pass

                time.sleep(1)

                # Capture initial DOM
                questions.extend(
                    self._parse_rendered_html(
                        page.content()
                    )
                )

                # Capture JSON responses
                for response in responses:

                    try:

                        data = response.json()

                        questions.extend(
                            self._recursive_question_search(
                                data
                            )
                        )

                    except Exception:
                        continue

                # Click Apply if application form is hidden
                if not questions:

                    self._click_apply(page)

                    time.sleep(1)

                    questions.extend(
                        self._parse_rendered_html(
                            page.content()
                        )
                    )

                # Walk through application steps
                for _ in range(self.max_steps):

                    before = len(
                        self._deduplicate_questions(
                            questions
                        )
                    )

                    questions.extend(
                        self._parse_rendered_html(
                            page.content()
                        )
                    )

                    if not self._click_next(
                        page
                    ):
                        break

                    time.sleep(0.8)

                    after = len(
                        self._deduplicate_questions(
                            questions
                        )
                    )

                    # If nothing changed and no next step,
                    # terminate.
                    if (
                        after == before
                        and not self._has_next_button(
                            page
                        )
                    ):
                        break

            finally:

                context.close()
                browser.close()

        return self._deduplicate_questions(
            questions
        )

    def _parse_rendered_html(
        self,
        html: str,
    ) -> list[dict]:

        soup = BeautifulSoup(
            html,
            "html.parser",
        )

        questions = []

        questions.extend(
            self._extract_json_state(soup)
        )

        questions.extend(
            self._extract_form_elements(soup)
        )

        return questions

    def _click_apply(self, page) -> bool:

        selectors = [
            "text=/^apply$/i",
            "text=/apply now/i",
            "button:has-text('Apply')",
            "a:has-text('Apply')",
        ]

        for selector in selectors:

            try:

                locator = page.locator(
                    selector
                ).first

                if locator.count() == 0:
                    continue

                if not locator.is_visible():
                    continue

                locator.click(
                    timeout=3000
                )

                return True

            except Exception:
                continue

        return False

    def _click_next(self, page) -> bool:

        # NEVER click submit.
        buttons = page.locator(
            "button, input[type='button'], "
            "a"
        )

        count = min(
            buttons.count(),
            100,
        )

        for index in range(count):

            try:

                button = buttons.nth(index)

                if not button.is_visible():
                    continue

                text = (
                    button.inner_text(
                        timeout=500
                    ).strip()
                )

                if not text:
                    text = (
                        button.get_attribute(
                            "value"
                        )
                        or ""
                    ).strip()

                if self.SUBMIT_TEXT.search(
                    text
                ):
                    continue

                if self.NEXT_TEXT.match(
                    text
                ):

                    button.click(
                        timeout=3000
                    )

                    return True

            except Exception:
                continue

        return False

    def _has_next_button(self, page) -> bool:

        buttons = page.locator(
            "button, input[type='button'], a"
        )

        count = min(
            buttons.count(),
            100,
        )

        for index in range(count):

            try:

                button = buttons.nth(index)

                if not button.is_visible():
                    continue

                text = (
                    button.inner_text(
                        timeout=300
                    ).strip()
                )

                if self.NEXT_TEXT.match(
                    text
                ):
                    return True

            except Exception:
                continue

        return False

    # ============================================================
    # DEDUPLICATION
    # ============================================================

    def _deduplicate_questions(
        self,
        questions: list[dict],
    ) -> list[dict]:

        clean = []
        seen = set()

        for question in questions:

            if not isinstance(
                question,
                dict,
            ):
                continue

            label = str(
                question.get(
                    "label",
                    "",
                )
            ).strip()

            if not label:
                continue

            key = re.sub(
                r"\s+",
                " ",
                label.lower(),
            )

            if key in seen:
                continue

            seen.add(key)

            question["label"] = label

            if not isinstance(
                question.get("options"),
                list,
            ):
                question["options"] = []

            clean.append(question)

        return clean
