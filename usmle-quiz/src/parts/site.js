/* ==========================================================================
   SITE SHELL: router, nav, home, dashboard, library, find palette, footer.
   The quiz itself (window.QuizApp) is mounted once and kept alive between tabs,
   so its timer, music and progress survive navigation.
   ========================================================================== */
(() => {
'use strict';
const { useState, useEffect, useLayoutEffect, useRef, useMemo, useCallback, useSyncExternalStore, Fragment } = React;
const html = htm.bind(React.createElement);
const cx = (...a) => a.filter(Boolean).join(' ');
const PROTECT_QUIZ = true;                       /* original copy / screenshot guards, scoped to the Quiz tab */
const reduceMotion = () => window.matchMedia('(prefers-reduced-motion: reduce)').matches || (typeof PrefsStore !== 'undefined' && PrefsStore.get().reduceMotion);
const hidePicture = (e) => { e.currentTarget.style.visibility = 'hidden'; };
const GALLERY_COLUMNS = (window.__quizData.gallery || []);
// Dark-academia medical / anatomical plates for the home gallery (center = the reaching-hand photo).
const ART = {
  center: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/center-reach.png',
  plates: [
    'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/art-lovers.png', 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/art-harp.png', 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/anat-infant-angiogram.png', 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/art-snake-apple.png',
    'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/center-reach.png',
    'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/anat-heart-lungs.png', 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/anat-viscera.png', 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/art-fingerprint.png', 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/anat-hand.png',
  ],
  heartLungs: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/anat-heart-lungs.png',
  viscera: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/anat-viscera.png',
  angiogram: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/anat-infant-angiogram.png',
  hand: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/anat-hand.png',
  fingerprint: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/art-fingerprint.png',
  snakeApple: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/art-snake-apple.png',
  harp: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/art-harp.png',
  lovers: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@ab61ede578743aa0133d6278b2f706746f98e45c/usmle-quiz/assets/anatomy/art-lovers.png',
};

const CARAV = {
  poster: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@48d9b2ea173475f0686b2f1e324974dd8f8fc0d3/usmle-quiz/assets/art/caravaggio-poster-hd.jpg',
  fill: 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@48d9b2ea173475f0686b2f1e324974dd8f8fc0d3/usmle-quiz/assets/art/caravaggio-fill-hd.jpg',
};

/* ---------- content, derived from the quiz so there is one source of truth ---------- */
const { questions: QS, explanations: EX } = window.__quizData;
const normExp = (e) => (typeof e === 'string' ? { text: e, images: [] } : { text: e.text, images: e.images || [] });
const CASE_META = [
  { code: 'CASE_001', tag: 'Urology · Anatomy', title: 'Prostate on CT', short: 'Prostate', wiki: 'Benign prostatic hyperplasia', cls: '' },
  { code: 'CASE_002', tag: 'Urology · Psychiatry', title: 'Erectile dysfunction', short: 'Erectile', wiki: 'Erectile dysfunction', cls: 'c2' },
  { code: 'CASE_003', tag: 'Urology · Embryology', title: 'Testicular torsion', short: 'Torsion', wiki: 'Testicular torsion', cls: 'c3' },
];
const EXTRA_META = window.__quizData.meta || {};
const EXTRA_WIKI = ['Rheumatoid arthritis', 'Brainstem', 'Omeprazole', 'Cardiac output', 'Spleen'];
const EXTRA_SHORT = ['Hands', 'Brainstem', 'pH curves', 'Graph', 'CT'];
const SWATCH = ['', 'c2', 'c3', 'c4'];
const CASES = QS.map((q, i) => {
  const exp = normExp(EX[i]);
  const m = exp.text.match(/Educational objective:\s*([\s\S]*)$/);
  const base = CASE_META[i] || (() => {
    const x = EXTRA_META[i] || {};
    return { code: 'CASE_00' + (i + 1), tag: 'USMLE sample · Item ' + (x.item || '?'), title: x.title || 'Sample question ' + (i + 1), short: EXTRA_SHORT[i - 3] || 'Case', wiki: EXTRA_WIKI[i - 3] || 'Medicine' };
  })();
  return { ...base, cls: SWATCH[i % 4], tabx: 8 + ((i * 29) % 56), id: 'q' + (i + 1), index: i, q, exp, objective: m ? m[1].trim() : '' };
});
const DURATION = 1800;

/* ---------- helpers ---------- */
const pad2 = (n) => String(n).padStart(2, '0');
const clock = (s) => { s = Math.max(0, Math.round(s || 0)); return `${pad2(Math.floor(s / 60))}:${pad2(s % 60)}`; };
const pctOf = (a) => Math.round((a.score / a.total) * 100);
const dayKey = (t) => new Date(t).toDateString();
const fmtDate = (t) => new Date(t).toLocaleDateString(undefined, { day: 'numeric', month: 'short' });
const fmtTime = (t) => new Date(t).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' });

function summarize(list) {
  const n = list.length;
  const best = n ? Math.max(...list.map(pctOf)) : 0;
  const avg = n ? Math.round(list.reduce((s, a) => s + pctOf(a), 0) / n) : 0;
  const time = list.reduce((s, a) => s + a.timeUsed, 0);
  const days = new Set(list.map(a => dayKey(a.timestamp)));
  let streak = 0;
  const d = new Date();
  if (!days.has(d.toDateString())) d.setDate(d.getDate() - 1);
  while (days.has(d.toDateString())) { streak++; d.setDate(d.getDate() - 1); }
  const today = list.filter(a => dayKey(a.timestamp) === new Date().toDateString()).length;
  return { n, best, avg, time, streak, today, last: n ? pctOf(list[n - 1]) : 0, prev: n > 1 ? pctOf(list[n - 2]) : null };
}

const SAMPLE = (() => {
  const day = 864e5, now = Date.now();
  const scores = [1, 1, 2, 2, 1, 2, 3, 3];
  return scores.map((s, i) => ({
    score: s, total: 3, sample: true,
    timeUsed: 700 + ((i * 197) % 380) - i * 24,
    timestamp: now - (scores.length - i) * day * 1.25,
    correct: [0, 1, 2].filter(k => (s === 3) || (s === 2 && k !== (i % 3)) || (s === 1 && k === (i % 3))),
  }));
})();

/* ---------- hooks ---------- */
const useAttempts = () => useSyncExternalStore(window.AttemptStore.subscribe, window.AttemptStore.get);

const ROUTES = ['home', 'quiz', 'dashboard', 'study', 'library', 'community', 'account'];
function useRoute() {
  const read = () => { const r = location.hash.replace(/^#\/?/, '').split('?')[0]; if (/^u\/[A-Za-z0-9_]{3,20}$/.test(r)) return 'community'; return ROUTES.includes(r) ? r : (r === '' && typeof PrefsStore !== 'undefined' && ROUTES.includes(PrefsStore.get().landing) ? PrefsStore.get().landing : 'home'); };
  const [route, setRoute] = useState(read);
  useEffect(() => {
    const on = () => setRoute(read());
    window.addEventListener('hashchange', on);
    return () => window.removeEventListener('hashchange', on);
  }, []);
  const go = useCallback((r) => { if (location.hash !== '#/' + r) location.hash = '#/' + r; else window.scrollTo({ top: 0, behavior: 'smooth' }); }, []);
  return [route, go];
}

function usePref(key, fallback) {
  const k = 'usmle-site:' + key;
  const [v, setV] = useState(() => { try { const r = localStorage.getItem(k); return r === null ? fallback : JSON.parse(r); } catch { return fallback; } });
  const set = useCallback((n) => { setV(n); try { localStorage.setItem(k, JSON.stringify(n)); } catch { /* ignore */ } }, [k]);
  return [v, set];
}

/* ---------- small external stores, shared by every component ---------- */
function makeStore(key, fallback) {
  let value;
  try { const raw = localStorage.getItem(key); value = raw === null ? fallback : JSON.parse(raw); } catch { value = fallback; }
  const subs = new Set();
  return {
    get: () => value,
    set(next) { value = next; try { localStorage.setItem(key, JSON.stringify(next)); } catch { /* storage unavailable */ } subs.forEach((f) => f()); },
    subscribe(fn) { subs.add(fn); return () => subs.delete(fn); },
  };
}
const UserStore = makeStore('usmle-site:displayName', 'Charles');   /* becomes the real name once there are logins */
const SavedStore = makeStore('usmle-site:saved', []);                /* bookmarked Wikipedia pages */
const LibTab = makeStore('usmle-site:libTab', 'results');
const AnnStore = makeStore('usmle-site:annotations', {});            /* { [url]: { hl: [text], note } } */
const annOf = (all, url) => all[url] || { hl: [], note: '' };
const setAnn = (url, patch) => { const all = AnnStore.get(); AnnStore.set({ ...all, [url]: { ...annOf(all, url), ...patch } }); };
const HL_COLORS = { y: '#FFE066', g: '#9BE7A5', p: '#FFB3D1', b: '#A8C4FF' };
const hlList = (ann) => ann.hl.map((h) => (typeof h === 'string' ? { t: h, c: 'y', n: '' } : h));
function withHighlights(text, hls, active) {
  let parts = [text];
  hls.forEach((h) => { parts = parts.flatMap((p) => typeof p !== 'string' ? [p] : p.split(h.t).flatMap((seg, i, arr) => (i < arr.length - 1 ? [seg, { m: h }] : [seg]))); });
  return parts.map((p, i) => (typeof p === 'string' ? p : html`<mark key=${i} className=${cx('hlm', 'c-' + p.m.c, active === p.m.t && 'active')} title=${p.m.n ? p.m.n : 'Click to open the note'} data-h=${p.m.t}>${p.m.t}</mark>`));
}
const useUserName = () => useSyncExternalStore(UserStore.subscribe, UserStore.get);
const useSaved = () => useSyncExternalStore(SavedStore.subscribe, SavedStore.get);

const ToastStore = (() => {
  let list = [], id = 0;
  const subs = new Set();
  const emit = () => subs.forEach((f) => f());
  return {
    get: () => list,
    subscribe(fn) { subs.add(fn); return () => subs.delete(fn); },
    push(msg) { const t = { id: ++id, msg }; list = [...list, t]; emit(); setTimeout(() => { list = list.filter((x) => x.id !== t.id); emit(); }, 2800); },
  };
})();
const toast = (msg) => ToastStore.push(msg);
function ToastHost() {
  const list = useSyncExternalStore(ToastStore.subscribe, ToastStore.get);
  return html`<div className="toasts" aria-live="polite">${list.map((t) => html`<div key=${t.id} className="toast">${t.msg}</div>`)}</div>`;
}

function copyText(text, okMsg) {
  const done = () => toast(okMsg || 'Copied');
  if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(text).then(done).catch(() => toast('Could not copy'));
  else toast('Could not copy');
}

const isSaved = (list, url) => list.some((p) => p.url === url);
function toggleSaved(page) {
  const list = SavedStore.get();
  if (isSaved(list, page.url)) { SavedStore.set(list.filter((p) => p.url !== page.url)); toast('Removed from your saved pages'); return; }
  SavedStore.set([{ title: page.title, extract: page.extract || '', thumb: page.thumb || '', url: page.url, source: page.source || 'Wikipedia', meta: page.meta || '', authors: page.authors || '', pmcid: page.pmcid || '', note: '', at: Date.now() }, ...list]);
  toast('Saved. It will be waiting for you in the Library.');
}
const setSavedNote = (url, note) => SavedStore.set(SavedStore.get().map((p) => (p.url === url ? { ...p, note } : p)));

/* "Dear Charles," — the name is editable in place and remembered. */
function NameField({ plain }) {
  const name = useUserName();
  return html`<span className=${cx('name-field', plain && 'nf-plain')}>
    <span className="nf-sizer" aria-hidden="true">${name || 'friend'}</span>
    <input value=${name} maxLength="24" spellCheck="false" aria-label="Your name" title="Click to change your name"
      onChange=${(e) => UserStore.set(e.target.value.slice(0, 24))}
      onBlur=${(e) => { if (!e.target.value.trim()) UserStore.set('friend'); }} />
    <svg className="nf-line" viewBox="0 0 100 10" preserveAspectRatio="none" aria-hidden="true"><path d="M2,6 C18,2 30,9 48,5 S80,3 98,6" /></svg>
  </span>`;
}

/* ---------- Wikipedia (public REST API, no key) ---------- */
const wikiMem = {};
async function wikiSummary(title) {
  if (wikiMem[title]) return wikiMem[title];
  try { const c = sessionStorage.getItem('wiki:' + title); if (c) return (wikiMem[title] = JSON.parse(c)); } catch { /* ignore */ }
  const r = await fetch('https://en.wikipedia.org/api/rest_v1/page/summary/' + encodeURIComponent(title.replace(/ /g, '_')));
  if (!r.ok) throw new Error('wiki ' + r.status);
  const j = await r.json();
  const out = { extract: j.extract, desc: j.description, thumb: j.thumbnail && j.thumbnail.source, url: j.content_urls && j.content_urls.desktop && j.content_urls.desktop.page };
  wikiMem[title] = out;
  try { sessionStorage.setItem('wiki:' + title, JSON.stringify(out)); } catch { /* ignore */ }
  return out;
}

async function wikiSearch(q, signal, n = 9) {
  const url = 'https://en.wikipedia.org/w/api.php?action=query&generator=search&gsrsearch=' + encodeURIComponent(q) +
    '&gsrlimit=' + n + '&prop=pageimages%7Cextracts&exintro=1&explaintext=1&exchars=300&piprop=thumbnail&pithumbsize=480&format=json&formatversion=2&origin=*';
  const r = await fetch(url, { signal });
  if (!r.ok) throw new Error('wiki ' + r.status);
  const j = await r.json();
  const pages = (j.query && j.query.pages) || [];
  return pages.sort((a, b) => a.index - b.index).map((p) => ({
    title: p.title, extract: p.extract || '', thumb: p.thumbnail && p.thumbnail.source,
    url: 'https://en.wikipedia.org/wiki/' + encodeURIComponent(p.title.replace(/ /g, '_')),
  }));
}
async function wikiFull(title) {
  const url = 'https://en.wikipedia.org/w/api.php?action=query&prop=extracts%7Cpageimages&exintro=1&explaintext=1&piprop=thumbnail&pithumbsize=900&redirects=1&titles=' +
    encodeURIComponent(title) + '&format=json&formatversion=2&origin=*';
  const r = await fetch(url);
  if (!r.ok) throw new Error('wiki ' + r.status);
  const p = (((await r.json()).query || {}).pages || [])[0] || {};
  return { extract: p.extract || '', thumb: p.thumbnail && p.thumbnail.source };
}

/* ---- journals: PubMed (abstracts) and Europe PMC (open-access full text) — both free, no key ---- */
const stripTags = (t) => (t || '').replace(/<[^>]+>/g, '').replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/\s+/g, ' ').trim();
const shortAuthors = (list, more) => list.filter(Boolean).slice(0, 3).join(', ') + (more ? ', et al.' : '');

async function pubmedSearch(q, signal, n = 8) {
  const E = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/';
  const s = await fetch(E + 'esearch.fcgi?db=pubmed&retmode=json&sort=relevance&retmax=' + n + '&term=' + encodeURIComponent(q), { signal });
  if (!s.ok) throw new Error('pubmed ' + s.status);
  const ids = ((await s.json()).esearchresult || {}).idlist || [];
  if (!ids.length) return [];
  const f = await fetch(E + 'efetch.fcgi?db=pubmed&retmode=xml&id=' + ids.join(','), { signal });
  if (!f.ok) throw new Error('pubmed ' + f.status);
  const doc = new DOMParser().parseFromString(await f.text(), 'text/xml');
  const txt = (el, sel) => { const x = el.querySelector(sel); return x ? x.textContent : ''; };
  return Array.from(doc.querySelectorAll('PubmedArticle')).map((a) => {
    const pmid = txt(a, 'MedlineCitation > PMID');
    const abstract = Array.from(a.querySelectorAll('Abstract > AbstractText')).map((t) => (t.getAttribute('Label') ? t.getAttribute('Label') + ': ' : '') + t.textContent).join('\n');
    const people = Array.from(a.querySelectorAll('AuthorList > Author')).map((x) => (txt(x, 'LastName') + ' ' + txt(x, 'Initials')).trim());
    const year = txt(a, 'JournalIssue > PubDate > Year') || txt(a, 'PubDate > MedlineDate').slice(0, 4);
    return { title: stripTags(txt(a, 'ArticleTitle')), extract: abstract || 'This paper has no abstract on PubMed. Open it to read the original.', thumb: '', source: 'PubMed',
             url: 'https://pubmed.ncbi.nlm.nih.gov/' + pmid + '/', meta: [txt(a, 'Journal > Title'), year].filter(Boolean).join(' · '), authors: shortAuthors(people, people.length > 3) };
  });
}

async function epmcSearch(q, signal, n = 8) {
  const url = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search?format=json&resultType=core&sort=RELEVANCE&pageSize=' + n + '&query=' + encodeURIComponent(q + ' AND OPEN_ACCESS:y');
  let r = await fetch(url, { signal });
  if (r.status === 503 || r.status === 429) { await new Promise((ok) => setTimeout(ok, 800)); r = await fetch(url, { signal }); }   /* Europe PMC briefly says 503 when busy */
  if (!r.ok) throw new Error('epmc ' + r.status);
  const list = ((await r.json()).resultList || {}).result || [];
  return list.map((x) => ({
    title: stripTags(x.title), extract: stripTags(x.abstractText) || 'Open this paper to read the full text.', thumb: '', source: 'Europe PMC',
    url: 'https://europepmc.org/article/' + x.source + '/' + x.id, pmcid: x.pmcid || '',
    meta: [x.journalInfo && x.journalInfo.journal && x.journalInfo.journal.title, x.pubYear].filter(Boolean).join(' · '),
    authors: stripTags(x.authorString || '').replace(/\.$/, '').split(', ').slice(0, 3).join(', ') + ((x.authorString || '').split(', ').length > 3 ? ', et al.' : ''),
  }));
}

const SOURCES = [['all', 'Everything'], ['wiki', 'Wikipedia'], ['pubmed', 'PubMed'], ['epmc', 'Open-access papers']];
const withTimeout = (p, ms) => Promise.race([p, new Promise((_, no) => setTimeout(() => no(new Error('timeout')), ms))]);

/* "Everything" shows results as each source answers, so one slow source never holds the others back. */
async function searchSources(q, source, signal, per, onUpdate) {
  const tag = (name, meta) => (r) => r.map((p) => ({ ...p, source: name, meta }));
  const jobs = {
    wiki: () => wikiSearch(q, signal, per || (source === 'all' ? 4 : 9)).then(tag('Wikipedia', 'Encyclopedia')),
    pubmed: () => pubmedSearch(q, signal, per || (source === 'all' ? 4 : 9)),
    epmc: () => epmcSearch(q, signal, per || (source === 'all' ? 4 : 9)),
  };
  if (source !== 'all') return withTimeout(jobs[source](), 12000);
  const order = ['wiki', 'pubmed', 'epmc'];
  const got = {}; let failed = 0;
  const merge = () => {
    const out = [], seen = new Set(), lists = order.map((k) => got[k] || []);
    const most = Math.max(0, ...lists.map((l) => l.length));
    for (let i = 0; i < most; i++) lists.forEach((l) => { const p = l[i]; if (p && !seen.has(p.url)) { seen.add(p.url); out.push(p); } });
    return out;
  };
  await Promise.all(order.map((k) => withTimeout(jobs[k](), 12000).then((r) => { got[k] = r; if (onUpdate) onUpdate(merge()); }).catch(() => { failed++; })));
  if (failed === order.length) throw new Error('all sources failed');
  return merge();
}

/* Full text for the reader: Europe PMC open-access papers come as JATS XML. */
const paperMem = {};
const paperFull = (pmcid) => paperMem[pmcid] || (paperMem[pmcid] = paperFullRaw(pmcid).catch((e) => { delete paperMem[pmcid]; throw e; }));
const firstFigure = (pmcid) => paperFull(pmcid).then((b) => { const f = b.find((x) => x.img); return f ? f.img : ''; });
const topicMem = {};
const topicImage = (q) => topicMem[q] || (topicMem[q] = wikiSearch(q, undefined, 4).then((r) => { const x = r.find((p) => p.thumb); return x ? x.thumb : ''; }).catch(() => ''));
async function paperFullRaw(pmcid) {
  const r = await fetch('https://www.ebi.ac.uk/europepmc/webservices/rest/' + pmcid + '/fullTextXML');
  if (!r.ok) throw new Error('fulltext ' + r.status);
  const doc = new DOMParser().parseFromString(await r.text(), 'text/xml');
  const body = doc.querySelector('body'); const blocks = [];
  const clean = (t) => t.replace(/\s+/g, ' ').trim();
  const figure = (el) => {
    const g = el.querySelector('graphic'); const ref = g && (g.getAttribute('xlink:href') || g.getAttribute('href')); if (!ref) return;
    const cap = el.querySelector('caption'); const lab = el.querySelector('label');
    blocks.push({ img: 'https://europepmc.org/articles/' + pmcid + '/bin/' + ref + '.jpg', cap: clean((lab ? lab.textContent + ' ' : '') + (cap ? cap.textContent : '')) });
  };
  const walk = (node, depth) => Array.from(node.children).forEach((el) => {
    if (el.tagName === 'sec') {
      const t = Array.from(el.children).find((c) => c.tagName === 'title');
      if (t) blocks.push({ h: clean(t.textContent), d: depth });
      walk(el, depth + 1);
    } else if (el.tagName === 'fig') figure(el);
    else if (el.tagName === 'p') {
      const figs = Array.from(el.querySelectorAll('fig')); const c = el.cloneNode(true); c.querySelectorAll('fig').forEach((f) => f.remove());
      const x = clean(c.textContent); if (x) blocks.push({ p: x }); figs.forEach(figure);
    }
    else if (el.tagName === 'list') Array.from(el.querySelectorAll('list-item')).forEach((li) => blocks.push({ p: '• ' + clean(li.textContent) }));
  });
  if (body) walk(body, 0);
  return blocks.slice(0, 500);
}

function useWiki(title, enabled) {
  const [data, setData] = useState(wikiMem[title] || null);
  const [failed, setFailed] = useState(false);
  useEffect(() => {
    if (!enabled || data) return;
    let live = true;
    wikiSummary(title).then(d => live && setData(d)).catch(() => live && setFailed(true));
    return () => { live = false; };
  }, [title, enabled]);
  return { data, failed };
}

/* ---------- tiny components ---------- */
function AN({ value, prefix = '', suffix = '', ms = 1300 }) {
  const ref = useRef(null);
  const [v, setV] = useState(reduceMotion() ? value : 0);
  useEffect(() => {
    if (reduceMotion()) { setV(value); return undefined; }
    let raf, started = false;
    const run = () => {
      if (started) return; started = true;
      const t0 = performance.now();
      const tick = (now) => { const t = Math.min(1, (now - t0) / ms); setV(Math.round(value * (1 - Math.pow(1 - t, 4)))); if (t < 1) raf = requestAnimationFrame(tick); };
      raf = requestAnimationFrame(tick);
    };
    const io = new IntersectionObserver((es) => { if (es.some(e => e.isIntersecting)) { run(); io.disconnect(); } }, { threshold: 0.2 });
    if (ref.current) io.observe(ref.current);
    return () => { io.disconnect(); cancelAnimationFrame(raf); };
  }, [value, ms]);
  return html`<span ref=${ref}>${prefix}${v}${suffix}</span>`;
}
const timeAgo = (t) => { const d = Math.max(0, Date.now() - t); if (d < 60000) return 'now'; if (d < 3600000) return Math.floor(d / 60000) + 'm'; if (d < 86400000) return Math.floor(d / 3600000) + 'h'; if (d < 2592000000) return Math.floor(d / 86400000) + 'd'; return Math.floor(d / 2592000000) + 'mo'; };
const chatTime = (t) => { if (!t) return ''; const d = new Date(t), n = new Date(), diff = n - d; if (diff < 120000) return 'Just Now'; if (d.toDateString() === n.toDateString()) return d.toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' }).toLowerCase(); const y = new Date(n); y.setDate(y.getDate() - 1); if (d.toDateString() === y.toDateString()) return 'Yesterday'; return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' }); };
const Icon = ({ n, cls }) => html`<i className=${cx('fas', n, cls)} aria-hidden="true"></i>`;

function Seg({ options, value, onChange }) {
  const refs = useRef({});
  const [th, setTh] = useState({ x: 0, w: 0 });
  const measure = useCallback(() => { const el = refs.current[value]; if (el) setTh({ x: el.offsetLeft, w: el.offsetWidth }); }, [value]);
  useLayoutEffect(measure, [measure]);
  useEffect(() => { window.addEventListener('resize', measure); if (document.fonts) document.fonts.ready.then(measure); return () => window.removeEventListener('resize', measure); }, [measure]);
  return html`<div className="pillseg" role="group">
    <span className="pill-thumb" style=${{ width: th.w, transform: `translateX(${th.x}px)` }}></span>
    ${options.map(([k, label]) => html`<button key=${k} ref=${(el) => { refs.current[k] = el; }} className=${cx(value === k && 'on')} onClick=${() => onChange(k)}>${label}</button>`)}
  </div>`;
}

/* ==========================================================================
   NAV
   ========================================================================== */
const TABS = [['home', 'Home'], ['quiz', 'Quiz'], ['dashboard', 'Dashboard'], ['study', 'Study'], ['library', 'Library'], ['community', 'Community']];
const SVG = (kids) => html`<svg viewBox="0 0 24 24" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">${kids}</svg>`;
const NAVI = {
  home: () => SVG(html`<path d="M3.5 11.2 12 4l8.5 7.2"/><path d="M5.8 9.6v9.1a1.3 1.3 0 0 0 1.3 1.3h2.9v-5.2h4V20h2.9a1.3 1.3 0 0 0 1.3-1.3V9.6"/>`),
  quiz: () => SVG(html`<path d="M4.2 19.8l1-4.3L16.6 4.1a2 2 0 0 1 2.8 0l.5.5a2 2 0 0 1 0 2.8L8.5 18.8l-4.3 1z"/><path d="M14.6 6.1l3.3 3.3"/><path d="M13 20h7"/>`),
  dashboard: () => SVG(html`<rect x="3.5" y="12" width="4.2" height="8.2" rx="1.6"/><rect x="9.9" y="4" width="4.2" height="16.2" rx="1.6"/><rect x="16.3" y="8.6" width="4.2" height="11.6" rx="1.6"/>`),
  study: () => SVG(html`<path d="M12 3.2 3.2 7.9 12 12.6l8.8-4.7L12 3.2z"/><path d="M3.2 12.2 12 16.9l8.8-4.7"/><path d="M3.2 16.4 12 21.1l8.8-4.7"/>`),
  library: () => SVG(html`<path d="M12 6.6C10.4 5.1 8 4.5 4 4.5v13.2c4 0 6.4.6 8 2.1 1.6-1.5 4-2.1 8-2.1V4.5c-4 0-6.4.6-8 2.1z"/><path d="M12 6.6v13.2"/>`),
  community: () => SVG(html`<circle cx="12" cy="12" r="8.8"/><path d="M3.2 12h17.6"/><path d="M12 3.2c2.6 2.4 3.9 5.4 3.9 8.8s-1.3 6.4-3.9 8.8c-2.6-2.4-3.9-5.4-3.9-8.8S9.4 5.6 12 3.2z"/>`),
};

function SiteNav({ route, gate, go, openFind }) {
  const hide = route === 'quiz' && !gate;
  return html`<header className="site-nav" data-hide=${hide}>
    <div className="nav-actions">
      <${AccountButton} />
      <button className="find glass" onClick=${openFind} aria-label="Find"><${Icon} n="fa-magnifying-glass" /><span className="lbl">Find</span><kbd>⌘K</kbd></button>
      <button className="btn-white" onClick=${() => go('quiz')}>Start a quiz</button>
    </div>
  </header>`;
}

/* The nav folds into one glass "stack" button as you scroll down, and unfolds when you scroll up or tap it. */
function useNavCompact(route) {
  const [compact, setCompact] = useState(false);
  const hold = useRef(0);
  useEffect(() => {
    if (route === 'quiz') { setCompact(false); return undefined; }
    let last = window.scrollY, raf = 0;
    const step = () => {
      raf = 0;
      if (window.innerWidth <= 760 || !PrefsStore.get().collapseNav) { setCompact(false); return; }
      const y = window.scrollY, dy = y - last; last = y;
      if (y < 90) setCompact(false);
      else if (dy < -6) setCompact(false);
      else if (dy > 6 && Date.now() > hold.current) setCompact(true);
    };
    const on = () => { if (!raf) raf = requestAnimationFrame(step); };
    window.addEventListener('scroll', on, { passive: true });
    return () => { window.removeEventListener('scroll', on); cancelAnimationFrame(raf); };
  }, [route]);
  useEffect(() => { document.body.dataset.navc = compact ? '1' : '0'; }, [compact]);
  const open = () => { hold.current = Date.now() + 2600; setCompact(false); };
  return [compact, open];
}

function Tabs({ route, gate, go }) {
  const [compact, openNav] = useNavCompact(route === 'quiz' && gate ? 'gate' : route);
  const refs = useRef({});
  const [th, setTh] = useState({ x: 0, w: 0 });
  const measure = useCallback(() => { const el = refs.current[route]; setTh(el ? { x: el.offsetLeft, w: el.offsetWidth } : { x: 0, w: 0 }); }, [route]);
  useLayoutEffect(measure, [measure]);
  useEffect(() => { window.addEventListener('resize', measure); if (document.fonts) document.fonts.ready.then(measure); return () => window.removeEventListener('resize', measure); }, [measure]);
  return html`<${Fragment}>
  <button className="stack-btn glass" data-on=${compact} onClick=${openNav} aria-label="Open the menu" tabIndex=${compact ? 0 : -1}><span className="st-i"><i></i><i></i><i></i></span></button>
  <nav className="tabs glass" data-pos=${route === 'quiz' && !gate ? 'bottom' : 'top'} data-c=${compact} aria-label="Primary">
    <span className="tab-thumb" style=${{ width: th.w, transform: `translateX(${th.x}px)` }}></span>
    ${TABS.map(([r, label]) => html`<button key=${r} ref=${(el) => { refs.current[r] = el; }} className="tab" aria-label=${label} data-tip=${label}
      aria-current=${route === r ? 'page' : undefined} onClick=${() => go(r)}>${NAVI[r]()}</button>`)}
  </nav><//>`;
}

/* ==========================================================================
   HOME
   ========================================================================== */
const VOYAGE = [
  [ART.hand, 'A hand to hold'], [ART.angiogram, 'Every vessel, mapped'], [ART.heartLungs, 'Heart and lungs'], [ART.viscera, 'The whole body'],
  [ART.fingerprint, 'Look closer'], [ART.snakeApple, 'Knowledge has a cost'], [ART.harp, 'The body, composed'], [ART.lovers, 'Where care begins'],
].filter(([src]) => !!src);

/* Types a line like the quiz intro: "Dear Charles," ... pause ... "you're amazing." */
function useTyped(name) {
  const [st, setSt] = useState({ l1: 0, l2: 0, done: false });
  useEffect(() => {
    const L1 = ('Dear ' + name + ',').length, L2 = "you're amazing.".length;
    if (reduceMotion()) { setSt({ l1: L1, l2: L2, done: true }); return undefined; }
    let t, i = 0, j = 0;
    const one = () => { i += 1; setSt({ l1: i, l2: 0, done: false }); t = setTimeout(i < L1 ? one : () => { t = setTimeout(two, 1100); }, i < L1 ? 85 : 0); };
    const two = () => { j += 1; setSt({ l1: L1, l2: j, done: j >= L2 }); if (j < L2) t = setTimeout(two, 70); };
    t = setTimeout(one, 600);
    return () => clearTimeout(t);
  }, []);
  return st;
}

function Hero({ go }) {
  const n = VOYAGE.length;
  const name = useUserName();
  const ty = useTyped(name);
  const ref = useRef(null);
  useEffect(() => {
    let raf = 0;
    const paint = () => {
      raf = 0; const el = ref.current; if (!el) return;
      const r = el.getBoundingClientRect(), span = Math.max(1, r.height - window.innerHeight);
      el.style.setProperty('--p', Math.max(0, Math.min(1, -r.top / (span * 0.85))).toFixed(4));
    };
    const on = () => { if (!raf) raf = requestAnimationFrame(paint); };
    window.addEventListener('scroll', on, { passive: true }); window.addEventListener('resize', on); paint();
    return () => { window.removeEventListener('scroll', on); window.removeEventListener('resize', on); if (raf) cancelAnimationFrame(raf); };
  }, []);
  const l1 = ('Dear ' + name + ',').slice(0, ty.l1), l2 = "you're amazing.".slice(0, ty.l2);
  const typingL1 = ty.l2 === 0 && !ty.done, typingL2 = ty.l2 > 0 && !ty.done;
  return html`<section className="home-sec vg-hero" data-nav="dark" ref=${ref} style=${{ '--p': 0 }}>
    <div className="vg-pin">
      <div className="vg-top">
        <h1 className="vg-h"><span className="vg-l1">${ty.l1 >= ('Dear ' + name + ',').length ? html`Dear <${NameField} />,` : l1}${typingL1 && html`<i className="vg-caret"></i>`}</span><br /><span className="vg-l2">${l2}${typingL2 && html`<i className="vg-caret"></i>`}</span></h1>
        <div className="vg-more">
          <p className="vg-sub">Somewhere between the late nights and the cold coffee, you decided to become a doctor. This little room is free, and it's yours.</p>
          <div className="vg-cta">
            <button className="btn-white" onClick=${() => go('quiz')}>Start a quiz <${Icon} n="fa-arrow-right" /></button>
            <button className="btn-ghost" onClick=${() => go('library')}>Take a look around</button>
          </div>
        </div>
      </div>
      <div className="vg-stage" aria-hidden="true">
        <div className="vg-ring" style=${{ '--n': n, '--k': (2 * Math.sin(Math.PI / n)).toFixed(4) }}>
          ${VOYAGE.map(([src, cap], i) => html`<figure key=${i} className="vg-card" style=${{ '--a': ((i * 360) / n) + 'deg' }}>
            <img src=${src} alt="" onError=${hidePicture} /><figcaption>${cap}</figcaption></figure>`)}
        </div>
      </div>
    </div>
  </section>`;
}

/* Words sharpen from blurred grey to crisp white as you scroll (like the Bending Spoons manifesto). */
function Manifesto() {
  const ref = useRef(null);
  const wordsRef = useRef([]);
  useEffect(() => {
    const paintAll = (fn) => wordsRef.current.forEach((w, i, a) => w && fn(w, i, a.length));
    if (reduceMotion()) { paintAll((w) => { w.style.filter = 'none'; w.style.opacity = 1; w.style.color = '#F4F2EC'; }); return undefined; }
    let raf = 0;
    const paint = () => {
      raf = 0;
      const el = ref.current; if (!el) return;
      const vh = window.innerHeight, r = el.getBoundingClientRect();
      const progress = Math.max(0, Math.min(1, (vh * 0.82 - r.top) / (r.height * 0.72)));
      paintAll((w, i, N) => {
        const local = Math.max(0, Math.min(1, progress * N - i));
        w.style.filter = local >= 1 ? 'none' : `blur(${((1 - local) * 7).toFixed(2)}px)`;
        w.style.opacity = (0.16 + 0.84 * local).toFixed(3);
        const c = Math.round(120 + 124 * local);
        w.style.color = local >= 1 ? '#F4F2EC' : `rgb(${c},${c},${c - 4})`;
      });
    };
    const on = () => { if (!raf) raf = requestAnimationFrame(paint); };
    window.addEventListener('scroll', on, { passive: true });
    window.addEventListener('resize', on);
    paint();
    return () => { window.removeEventListener('scroll', on); window.removeEventListener('resize', on); cancelAnimationFrame(raf); };
  }, []);
  let wi = -1;
  const para = (segments) => {
    const words = [];
    segments.forEach((seg) => seg.t.split(/(\s+)/).forEach((tok) => { if (tok !== '') words.push({ tok, em: !!seg.em }); }));
    return html`<p className="bs-para">${words.map((w, k) => {
      if (/^\s+$/.test(w.tok)) return ' ';
      wi++; const idx = wi;
      const span = html`<span className="bs-word" ref=${(el) => { wordsRef.current[idx] = el; }}>${w.tok}</span>`;
      return w.em ? html`<em key=${k}>${span}</em>` : html`<${Fragment} key=${k}>${span}<//>`;
    })}</p>`;
  };
  return html`<section className="home-sec home-dark bs-manifesto seam-top" data-nav="dark" ref=${ref}>
    <div className="home-wrap">
      ${para([{ t: 'Nobody warns you how ' }, { t: 'heavy', em: true }, { t: " it gets. The long nights, the doubt, the little voice asking if you're enough." }])}
      ${para([{ t: 'You are. And someday a stranger will trust you with the hardest day of their life. That ' }, { t: 'trust', em: true }, { t: ' starts here, one question at a time.' }])}
    </div>
  </section>`;
}

const CELLS = [
  { k: 'Timed', title: "A clock that's on your side", body: 'Thirty quiet minutes, just like the real thing, with nobody watching.', bg: '#0E0E11', cls: 'on-dark', ico: 'fa-stopwatch', accent: true },
  { k: 'Imaging', title: 'Answers that actually teach', body: 'Every explanation shows its work, with the real images right beside it.', art: 'heartLungs', ico: 'fa-images' },
  { k: 'Tools', title: 'Little helpers, close by', body: 'A calculator sits next to the question, never on top of it.', bg: '#2B1E16', cls: 'on-dark', ico: 'fa-calculator' },
  { k: 'Focus', title: 'Music to settle into', body: 'Pick a playlist, press play, and let the room go quiet around you.', bg: '#4A3527', cls: 'on-dark', ico: 'fa-music' },
  { k: 'Progress', title: 'Your progress stays yours', body: "Your scores live on this device, so you can see how far you've come.", art: 'angiogram', ico: 'fa-chart-line' },
  { k: 'Library', title: 'Read more about any case', body: 'Search Wikipedia and journals, highlight what matters, and keep your notes.', bg: '#D9C3A0', cls: 'on-light', ico: 'fa-bookmark' },
];
function Inside({ go }) {
  return html`<section className="home-sec home-light seam-top" data-nav="light" style=${{ padding: 'clamp(90px,13vh,170px) 0' }}>
    <div className="home-wrap">
      <div className="bs-grid-head reveal">
        <h2>A small room with<br />everything <em>you need.</em></h2>
        <button className="btn-dark btn-sm" onClick=${() => go('quiz')}>Start a quiz <${Icon} n="fa-arrow-right" /></button>
      </div>
      <div className="bs-grid">
        ${CELLS.map((c, i) => html`<article key=${i} className=${cx('fcell reveal', c.art ? 'has-art' : c.cls)} style=${{ background: c.bg, '--d': i * 70 + 'ms' }}>
          ${c.art && html`<div className="fc-art"><img src=${ART[c.art]} alt="" loading="lazy" onError=${hidePicture} /></div>`}
          <div className="fc-k">${c.k}</div>
          <div>
            <div className="fc-ico" style=${c.accent ? { background: 'var(--coral)', color: '#1b0d06' } : undefined}><${Icon} n=${c.ico} /></div>
            <h3>${c.title}</h3>
            <p>${c.body}</p>
          </div>
        </article>`)}
      </div>
    </div>
  </section>`;
}

const ORBIT = [['fa-globe', 0], ['fa-flask', 0], ['fa-file-lines', 1], ['fa-cube', 0], ['fa-folder-open', 0], ['fa-highlighter', 1], ['fa-bookmark', 0], ['fa-bone', 0], ['fa-heart-pulse', 1], ['fa-brain', 0], ['fa-lungs', 0], ['fa-microscope', 1]];
function Orbit({ go }) {
  const n = ORBIT.length;
  return html`<section className="ob" data-nav="dark">
    <div className="ob-card reveal">
      <div className="ob-ring" aria-hidden="true">${ORBIT.map(([ic, hot], i) => html`<span key=${i} className=${cx('ob-tile', hot && 'hot')} style=${{ '--a': (i * 360) / n + 'deg' }}><i className=${'fas ' + ic}></i></span>`)}</div>
      <h2>Every source.<br /><span className="ob-o">One<svg viewBox="0 0 160 70" preserveAspectRatio="none" aria-hidden="true"><path d="M82,6 C30,3 4,22 7,40 C10,60 55,68 100,62 C146,56 158,32 134,15 C116,4 70,3 44,12" /></svg></span> place. Free.</h2>
      <div className="ob-foot">
        <p>Wikipedia, PubMed, open-access journals, 3D models and your own case files, together in one calm library. Read, highlight, save, and come back whenever you like.</p>
        <button className="lh-btn" onClick=${() => go('library')}>Open the Library <${Icon} n="fa-arrow-right" /></button>
      </div>
    </div>
  </section>`;
}

/* Sticky stacked cards: each one slides up and settles on top of the last. */
const STACK = [
  { n: '01', k: 'Show up', title: 'Start small. Just begin.', body: "Open a block, start the clock, and answer the first question. That's the whole job for today. The rest follows.", bg: '#0F0D0B', fg: '#F4F0E6', img: 'angiogram' },
  { n: '02', k: 'Learn the why', title: 'Read the reason behind every answer.', body: "Right or wrong, each question opens into an explanation with the real images beside it. That's where it really sticks.", bg: '#EFE9DA', fg: '#17120e', img: 'heartLungs' },
  { n: '03', k: 'Track your scores', title: 'See which topics need more time.', body: 'The dashboard shows your scores and the questions you miss, so you know where to spend tomorrow.', bg: '#3A2A20', fg: '#F4F0E6', img: 'fingerprint' },
  { n: '04', k: 'Come back tomorrow', title: 'Keep a steady pace.', body: 'Twenty minutes a day beats one long session a week. Your streak and saved pages are here when you return.', bg: '#C98A5E', fg: '#20130a', img: 'center' },
];
function Stack() {
  const refs = useRef([]);
  useEffect(() => {
    if (reduceMotion()) return undefined;
    let raf = 0;
    const paint = () => {
      raf = 0;
      const vh = window.innerHeight;
      refs.current.forEach((el, i) => {
        const next = refs.current[i + 1];
        const inner = el && el.firstChild;
        if (!inner || !next) return;
        const stickTop = 96 + i * 56;
        const p = Math.max(0, Math.min(1, 1 - (next.getBoundingClientRect().top - (stickTop + 56)) / (vh * 0.62)));
        inner.style.transform = `scale(${(1 - 0.045 * p).toFixed(4)})`;
        inner.style.filter = p > 0.01 ? `brightness(${(1 - 0.38 * p).toFixed(3)})` : 'none';
      });
    };
    const on = () => { if (!raf) raf = requestAnimationFrame(paint); };
    window.addEventListener('scroll', on, { passive: true });
    window.addEventListener('resize', on);
    paint();
    return () => { window.removeEventListener('scroll', on); window.removeEventListener('resize', on); cancelAnimationFrame(raf); };
  }, []);
  return html`<section className="home-sec home-dark seam-top" data-nav="dark" style=${{ padding: 'clamp(90px,13vh,170px) 0 clamp(60px,9vh,120px)' }}>
    <div className="home-wrap">
      <div className="stk-head reveal"><p className="kquiet">How it works</p><h2>Small steps, <em>every day.</em></h2></div>
      <div className="stk">
        ${STACK.map((c, i) => html`<article key=${i} className="stk-card" ref=${(el) => { refs.current[i] = el; }} style=${{ '--i': i }}>
          <div className="stk-in" style=${{ '--bg': c.bg, '--fg': c.fg }}>
            <div className="stk-txt">
              <div className="stk-top"><span className="stk-num">${c.n}</span><span className="stk-k">${c.k}</span></div>
              <div><h3>${c.title}</h3><p>${c.body}</p></div>
            </div>
            <div className="stk-img"><img src=${ART[c.img]} alt="" loading="lazy" onError=${hidePicture} /></div>
          </div>
        </article>`)}
      </div>
    </div>
  </section>`;
}

function Stats({ attempts }) {
  const s = summarize(attempts);
  const items = [
    { n: html`<${AN} value=${CASES.length} />`, k: 'Cases', l: 'with real images, and more soon' },
    { n: '30:00', k: 'Per block', l: 'on the clock, like exam day' },
    { n: s.n ? html`<${AN} value=${s.best} suffix="%" />` : html`<em>Free</em>`, k: s.n ? 'Personal best' : 'Always', l: s.n ? 'your highest score so far' : 'no sign-up, no cost' },
  ];
  return html`<section className="home-sec home-dark" data-nav="dark" style=${{ padding: 'clamp(70px,10vh,130px) clamp(18px,5vw,72px)', borderTop: '1px solid rgba(255,255,255,.07)' }}>
    <div className="home-wrap bs-stats">
      ${items.map((it, i) => html`<div key=${i} className="bs-stat reveal" style=${{ '--d': i * 90 + 'ms' }}><small>${it.k}</small><b>${it.n}</b><span>${it.l}</span></div>`)}
    </div>
  </section>`;
}

function CtaBig({ go }) {
  return html`<section className="home-sec bs-cta seam-top" data-nav="light">
    <div className="home-wrap">
      <h2 className="reveal">Whenever you're<br /><em>ready.</em></h2>
      <p className="reveal" style=${{ '--d': '100ms' }}>No sign-up and no pressure. Open a block, do your best, and come back tomorrow.</p>
      <div className="reveal" style=${{ '--d': '160ms' }}><button className="btn-dark btn-lg" onClick=${() => go('quiz')}>Start a quiz <${Icon} n="fa-arrow-right" /></button></div>
    </div>
  </section>`;
}

/* Editorial quote block (inspired by your "History" poster): condensed caps, a small paragraph, and
   a painting showing through giant letters. */
const ALPHA = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'.split('');
function QuoteBlock() {
  return html`<section className="home-sec home-dark qb" data-nav="dark">
    <div className="home-wrap">
      <div className="qb-grid">
        <blockquote className="qb-quote reveal"><p>The good physician treats the disease; the great physician treats the patient who has the disease.</p><cite>William Osler</cite></blockquote>
        <div className="qb-note reveal" style=${{ '--d': '120ms' }}>
          <p>Step 1 asks what you know. Everything after it asks who you are. So study hard, and be kind to yourself along the way, because the world needs the kind of doctor you're becoming.</p>
          <p>This site is free, for anyone walking the same road. No catch.</p>
          <p className="tiny">Artwork: Caravaggio (1571–1610), Supper at Emmaus, 1601. The painting is in the public domain.</p>
        </div>
      </div>
      <div className="qb-giant-wrap reveal" aria-hidden="true" style=${{ '--fill': `url(${CARAV.fill})` }}>
        <div className="qb-giant"><span>Becoming</span><span>a doctor</span></div>
        <div className="qb-spec a"><small>01</small><b>Bebas Neue</b><i>Headline typography</i><div className="qb-az">${ALPHA.map((c) => html`<span key=${c}>${c}</span>`)}</div></div>
        <div className="qb-spec b"><small>02</small><b>Montserrat</b><i>Body typography</i><div className="qb-az">${ALPHA.map((c) => html`<span key=${c}>${c}</span>`)}</div></div>
      </div>
    </div>
  </section>`;
}

/* ==========================================================================
   DASHBOARD  (layout after your Donezo reference, wired to your real attempts)
   ========================================================================== */
const HATCH_H = [58, 76, 64, 88, 82, 56, 72];
const WEEKDAYS = ['S', 'M', 'T', 'W', 'T', 'F', 'S'];
const CASE_ICONS = ['fa-droplet', 'fa-pills', 'fa-user-doctor', 'fa-hand', 'fa-brain', 'fa-flask', 'fa-heart-pulse', 'fa-x-ray'];
const CASE_TINTS = ['#8a5a3c', '#a8793f', '#5b6b57', '#7b4a4a', '#4f5d75', '#9a6b2f', '#6d4c41', '#556b6b'];
const AV_TINTS = ['#ecd3c2', '#dfd6b9', '#d9c9d6', '#f1e1b5', '#cfdcd6'];

function KpiCard({ title, value, suffix, foot, icon, feat, onGo, delay }) {
  return html`<article className=${cx('dz-kpi reveal', feat && 'feat')} style=${{ '--d': delay + 'ms' }}>
    <h4>${title}</h4>
    <button className="dz-arrow" onClick=${onGo} aria-label=${`Open ${title}`}><${Icon} n="fa-arrow-up" /></button>
    <div className="n"><${AN} value=${value} suffix=${suffix || ''} /></div>
    <div className="f"><i className=${cx('fas', icon)}></i>${foot}</div>
  </article>`;
}

function Gauge({ parts }) {
  /* semicircle, three arcs: mastered / learning / not yet (hatched) */
  const R = 86, LEN = Math.PI * R, total = parts.reduce((s, p) => s + p.v, 0) || 1;
  let acc = 0;
  return html`<svg viewBox="0 0 220 124" role="img" aria-label="Case mastery">
    <defs><pattern id="dzHatch" width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="3" height="7" fill="rgba(60,40,28,.4)" /></pattern></defs>
    <path d="M24,110 A86,86 0 0 1 196,110" fill="none" stroke="#e6dfd0" strokeWidth="34" strokeLinecap="round" />
    ${parts.filter((p) => p.v > 0).map((p, i) => {
      const seg = Math.max(0, (p.v / total) * LEN - 10), off = (acc / total) * LEN; acc += p.v;
      return html`<path key=${i} className="arc" d="M24,110 A86,86 0 0 1 196,110" stroke=${p.c} strokeDasharray=${`${seg} ${LEN}`} strokeDashoffset=${-off - 5} style=${{ '--len': LEN, animationDelay: i * 160 + 'ms' }} />`;
    })}
  </svg>`;
}

function FocusTimer() {
  const [running, setRunning] = useState(false);
  const [sec, setSec] = useState(0);
  const logged = useRef(0);
  const flush = (s) => { logFocus(s - logged.current); logged.current = s; };
  const log = useStore(FocusLog);
  useEffect(() => {
    if (!running) return undefined;
    const id = setInterval(() => setSec((s) => s + 1), 1000);
    return () => clearInterval(id);
  }, [running]);
  const hms = `${pad2(Math.floor(sec / 3600))}:${pad2(Math.floor((sec % 3600) / 60))}:${pad2(sec % 60)}`;
  return html`<article className="dz-track dz-w">
    <h4>Focus timer</h4>
    <div className="dz-clock" role="timer">${hms}</div>
    <div className="dz-ctl">
      <button className="go" onClick=${() => { if (running) flush(sec); setRunning((r) => !r); }} aria-label=${running ? 'Pause' : 'Start'}><${Icon} n=${running ? 'fa-pause' : 'fa-play'} /></button>
      <button className="stop" onClick=${() => { if (running) flush(sec); setRunning(false); setSec(0); logged.current = 0; }} aria-label="Reset"><${Icon} n="fa-stop" /></button>
    </div>
    <p className="dz-ft">${focusToday(log)} min focused today</p>
  </article>`;
}

/* Pillars made of one painting; they climb out of frame as you scroll. */
const BARS = [58, 74, 90, 100, 66, 78, 50];
function BarsIntro() {
  const ref = useRef(null);
  useEffect(() => {
    let raf = 0;
    const paint = () => {
      raf = 0; const el = ref.current; if (!el) return;
      const r = el.getBoundingClientRect(), span = Math.max(1, r.height - window.innerHeight);
      const p = Math.max(0, Math.min(1, -r.top / span));
      el.querySelectorAll('.bx-bar').forEach((b, i) => {
        const q = Math.max(0, Math.min(1, (p - i * 0.045) / (1 - (BARS.length - 1) * 0.045)));
        const e = q * q * (3 - 2 * q);
        b.style.setProperty('--s', (e * window.innerHeight * 1.12).toFixed(1) + 'px');
      });
      el.style.setProperty('--p', p.toFixed(3));
    };
    const on = () => { if (!raf) raf = requestAnimationFrame(paint); };
    window.addEventListener('scroll', on, { passive: true }); window.addEventListener('resize', on); paint();
    return () => { window.removeEventListener('scroll', on); window.removeEventListener('resize', on); cancelAnimationFrame(raf); };
  }, []);
  return html`<section className="bx" ref=${ref} data-nav="dark" style=${{ '--n': BARS.length }}>
    <div className="bx-pin">
      <h2 className="bx-word" aria-hidden="true">YOUR PROGRESS</h2>
      <div className="bx-bars">${BARS.map((h, i) => html`<div key=${i} className="bx-bar" style=${{ '--h': h, '--i': i }}><img src=${CARAV.fill} alt="" onError=${hidePicture} /></div>`)}</div>
      <p className="bx-hint">Scroll to open your dashboard <${Icon} n="fa-arrow-down" /></p>
    </div>
  </section>`;
}

function Dashboard({ go, openFind }) {
  const real = useAttempts();
  const saved = useSaved();
  const name = useUserName();
  const [demo, setDemo] = useState(false);
  const [goal, setGoal] = usePref('goal', 3);
  const [hover, setHover] = useState(null);
  const base = real.length ? real : (demo ? SAMPLE : []);
  const st = summarize(base);
  const last = base[base.length - 1];

  /* weekday bars: best score on each weekday */
  const byDay = Array(7).fill(null);
  base.forEach((a) => { const d = new Date(a.timestamp).getDay(); const p = pctOf(a); if (byDay[d] === null || p > byDay[d]) byDay[d] = p; });
  const dataVals = byDay.filter((v) => v !== null);
  const maxVal = dataVals.length ? Math.max(...dataVals) : null;
  const maxIdx = maxVal === null ? -1 : byDay.indexOf(maxVal);
  const activeBar = hover !== null ? hover : maxIdx;
  const todayIdx = new Date().getDay();

  /* case mastery from the latest attempt */
  const withData = base.filter((a) => Array.isArray(a.correct));
  const everRight = new Set(); withData.forEach((a) => a.correct.forEach((i) => everRight.add(i)));
  const lastRight = new Set(last && Array.isArray(last.correct) ? last.correct : []);
  const masteredN = CASES.filter((c) => lastRight.has(c.index)).length;
  const learningN = CASES.filter((c) => !lastRight.has(c.index) && everRight.has(c.index)).length;
  const notYetN = CASES.length - masteredN - learningN;
  const masteryPct = base.length ? Math.round((masteredN / CASES.length) * 100) : 0;

  const delta = st.prev !== null ? st.last - st.prev : null;
  const initials = (name || '?').trim().charAt(0).toUpperCase();
  const recent = base.slice(-4).reverse();
  const status = (a) => (a.timeUp ? ['low', "Time's up"] : pctOf(a) >= 70 ? ['', 'Strong'] : pctOf(a) >= 40 ? ['mid', 'Getting there'] : ['low', 'Needs review']);

  return html`<section className="dz-page" data-nav="light">
    <div className="dz-shell">
      <aside className="dz-side">
        <div className="dz-logo"><span className="brand-mark"><i className="fas fa-plus"></i></span>Step One</div>
        <div><div className="dz-lbl">MENU</div>
          <nav className="dz-nav">
            <button className="on"><${Icon} n="fa-table-cells-large" />Dashboard</button>
            <button onClick=${() => go('quiz')}><${Icon} n="fa-pen-to-square" />Quiz</button>
            <button onClick=${() => { LibTab.set('results'); go('library'); }}><${Icon} n="fa-folder-open" />Library</button>
            <button onClick=${() => { LibTab.set('saved'); go('library'); }}><${Icon} n="fa-bookmark" />Saved${saved.length > 0 && html`<span className="dz-badge">${saved.length}</span>`}</button>
          </nav></div>
        <div className="dz-gen"><div className="dz-lbl">GENERAL</div>
          <nav className="dz-nav">
            <button onClick=${() => setDemo((d) => !d)}><${Icon} n="fa-wand-magic-sparkles" />${demo ? 'Hide sample data' : 'Preview sample data'}</button>
            <button onClick=${openFind}><${Icon} n="fa-magnifying-glass" />Find anything</button>
            <button onClick=${() => { if (confirm('Clear every saved attempt on this device?')) { window.AttemptStore.clear(); toast('History cleared'); } }}><${Icon} n="fa-rotate-left" />Reset my history</button>
            <button onClick=${() => go('home')}><${Icon} n="fa-house" />Back home</button>
          </nav></div>
        <div className="dz-promo"><b>Keep reading</b><small>${saved.length ? `You have ${saved.length} saved page${saved.length === 1 ? '' : 's'} waiting.` : 'Save any page you love and it will wait for you here.'}</small>
          <button onClick=${() => { LibTab.set(saved.length ? 'saved' : 'results'); go('library'); }}>Open the library</button></div>
      </aside>

      <div className="dz-main">
        <header className="dz-top">
          <button className="dz-search" onClick=${openFind}><${Icon} n="fa-magnifying-glass" />Search anything<kbd>⌘ K</kbd></button>
          <div className="dz-spacer"></div>
          <button className="dz-ic" onClick=${() => toast("No new messages. Just you and the work, and that's a good place to be.")} aria-label="Messages"><${Icon} n="fa-envelope" /></button>
          <button className="dz-ic" onClick=${() => toast(st.streak ? `You're on a ${st.streak}-day streak. Keep it going.` : 'Start today and your streak begins.')} aria-label="Notifications"><${Icon} n="fa-bell" /></button>
          <div className="dz-user"><span className="dz-av">${initials}</span><div><b><${NameField} plain=${true} /></b><small>Free · saved on this device</small></div></div>
        </header>

        <div className="dz-body">
          <div className="dz-title">
            <div><h1>Dashboard</h1><p>${st.n ? "Here's how far you've come. Be proud of it." : 'Plan, practise, and see how far you go.'}</p></div>
            <div className="dz-acts"><button className="dz-btn solid" onClick=${() => go('quiz')}><${Icon} n="fa-plus" />Start a quiz</button><button className="dz-btn out" onClick=${() => { LibTab.set('results'); go('library'); }}>Open the library</button></div>
          </div>

          ${!real.length && html`<div className="dz-note"><span>${demo ? 'This is sample data, so you can see how the page fills up.' : "Nothing here yet. Take your first quiz and this page fills itself in, just for you."}</span>
            <span style=${{ display: 'flex', gap: 8 }}><button className="dz-btn solid sm" onClick=${() => go('quiz')}>Take the quiz</button><button className="dz-btn out sm" onClick=${() => setDemo((d) => !d)}>${demo ? 'Hide sample' : 'Preview sample'}</button></span></div>`}

          <div className="dz-kpis">
            <${KpiCard} feat=${true} title="Total attempts" value=${st.n} foot=${last ? `Last one was ${fmtDate(last.timestamp)}` : 'A fresh start'} icon="fa-clock" delay=${0} onGo=${() => go('quiz')} />
            <${KpiCard} title="Best score" value=${st.best} suffix="%" foot=${st.n ? `across ${st.n} attempt${st.n === 1 ? '' : 's'}` : 'waiting for you'} icon="fa-trophy" delay=${70} onGo=${() => go('quiz')} />
            <${KpiCard} title="Average score" value=${st.avg} suffix="%" foot=${delta === null ? 'Keep going' : delta === 0 ? 'Holding steady' : delta > 0 ? `Up ${delta} pts last time` : `Down ${Math.abs(delta)} pts last time`} icon=${delta !== null && delta < 0 ? 'fa-arrow-trend-down' : 'fa-arrow-trend-up'} delay=${140} onGo=${() => go('quiz')} />
            <${KpiCard} title="Day streak" value=${st.streak} foot=${`${st.today} session${st.today === 1 ? '' : 's'} today`} icon="fa-fire" delay=${210} onGo=${() => go('quiz')} />
          </div>

          <div className="dz-grid">
            <article className="dz-card dz-a reveal" style=${{ '--d': '80ms' }}>
              <div className="dz-ch"><div><h4>Practice analytics</h4><div className="sub">Your best score on each day of the week</div></div></div>
              <div className="dz-bars" onPointerLeave=${() => setHover(null)}>
                ${WEEKDAYS.map((d, i) => {
                  const v = byDay[i]; const has = v !== null; const h = (has ? Math.max(30, v) : HATCH_H[i]) * 0.8;
                  return html`<div key=${i} className=${cx('dz-bc', activeBar === i && has && 'on', todayIdx === i && 'today')} onPointerEnter=${() => has && setHover(i)}>
                    ${has && html`<span className="dz-bub" style=${{ '--h': h }}>${v}%</span>`}
                    <div className=${cx('dz-bar', has && 'has', has && i === maxIdx && 'max')} style=${{ height: h + '%', '--i': i }}></div>
                    <span className="l">${d}</span></div>`;
                })}
              </div>
            </article>

            <article className="dz-card dz-r dz-rem reveal" style=${{ '--d': '120ms' }}>
              <h4>Today's block</h4>
              <h5>Take a quiet half hour</h5>
              <p>30:00 on the clock · ${CASES.length} cases</p>
              <div className="dz-step">Goal ${st.today} / ${goal} <button onClick=${() => setGoal(Math.max(1, goal - 1))} aria-label="Fewer">−</button><button onClick=${() => setGoal(Math.min(12, goal + 1))} aria-label="More">+</button></div>
              <button className="dz-btn solid" onClick=${() => go('quiz')}><${Icon} n="fa-play" />Start a session</button>
            </article>

            <article className="dz-card dz-p reveal" style=${{ '--d': '160ms' }}>
              <div className="dz-ch"><h4>Cases</h4><button className="dz-btn out sm" onClick=${() => { LibTab.set('results'); go('library'); }}>+ Library</button></div>
              <div className="dz-list">
                ${CASES.slice(0, 5).map((c) => { const ok = withData.filter((a) => a.correct.includes(c.index)).length;
                  return html`<button key=${c.id} className="dz-li" onClick=${() => go('library')}><span className="dz-pi" style=${{ background: CASE_TINTS[c.index % 8] }}><${Icon} n=${CASE_ICONS[c.index % 8]} /></span>
                    <span><b>${c.title}</b><small>${withData.length ? `Right ${ok} of ${withData.length} times` : 'Not tried yet'}</small></span></button>`; })}
              </div>
            </article>

            <article className="dz-card dz-t reveal" style=${{ '--d': '200ms' }}>
              <div className="dz-ch"><div><h4>Recent sessions</h4><div className="sub">Your last few attempts</div></div><button className="dz-btn out sm" onClick=${() => go('quiz')}>+ Another</button></div>
              ${recent.length ? recent.map((a, i) => { const [cls, label] = status(a); const num = base.length - i;
                return html`<div key=${a.timestamp + '-' + i} className="dz-row"><span className="av" style=${{ background: AV_TINTS[i % 5] }}>#${num}</span>
                  <div><b>Attempt ${num} · ${a.score}/${a.total} correct</b><small>${fmtDate(a.timestamp)} · ${clock(a.timeUsed)}</small></div><span className=${cx('pill', cls)}>${label}</span></div>`; })
                : html`<p className="sub" style=${{ padding: '22px 0', fontSize: 14 }}>When you finish a quiz, it lands right here.</p>`}
            </article>

            <article className="dz-card dz-g reveal" style=${{ '--d': '240ms' }}>
              <div className="dz-ch"><div><h4>Case mastery</h4><div className="sub">From your latest attempt</div></div></div>
              <div className="dz-gauge">
                <${Gauge} parts=${[{ v: masteredN, c: '#2a1d14' }, { v: learningN, c: '#b0825c' }, { v: notYetN, c: 'url(#dzHatch)' }]} />
                <div className="mid"><b>${masteryPct}%</b><small>Mastered</small></div>
              </div>
              <div className="dz-legend"><span><i style=${{ background: '#2a1d14' }}></i>Mastered</span><span><i style=${{ background: '#b0825c' }}></i>Learning</span><span><i style=${{ background: 'repeating-linear-gradient(135deg,#8f8579 0 2px,#fff 2px 4px)' }}></i>Not yet</span></div>
            </article>

            <${FocusTimer} />
          </div>
        </div>
      </div>
    </div>
  </section>`;
}

/* ==========================================================================
   LIBRARY: folder-tab case files + reader + Wikipedia explorer
   ========================================================================== */
function CaseFile({ c, active, onActivate, onRead }) {
  const [seen, setSeen] = useState(active);
  useEffect(() => { if (active) setSeen(true); }, [active]);
  const { data, failed } = useWiki(c.wiki, seen);
  const saved = useSaved();
  const onKey = (e) => { if (!active && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); onActivate(); } };
  return html`<div className=${cx('file', c.cls, active && 'active')} style=${{ '--tabx': c.tabx + '%' }} role="button" tabIndex=${active ? -1 : 0} aria-expanded=${active} aria-label=${`Open ${c.title}`} onClick=${() => !active && onActivate()} onKeyDown=${onKey}>
    <span className="tab-n">${c.code} //</span>
    <div className="spine"><span>0${c.index + 1}</span><b>${c.short}</b><span>${c.tag.split(' · ')[0]}</span></div>
    <div className="body">
      <p className="eyebrow" style=${{ color: 'rgba(35,35,26,.65)' }}>${c.code} // ${c.tag}</p>
      <h3>${c.title}</h3>
      <div className="wiki">${data && data.thumb ? html`<img src=${data.thumb} alt="" loading="lazy" />` : html`<div className="ph"></div>`}
        <div><b style=${{ fontFamily: 'Space Grotesk', fontSize: 11, letterSpacing: '.14em', textTransform: 'uppercase' }}>Wikipedia</b><br />${data ? data.extract.slice(0, 230) + (data.extract.length > 230 ? '…' : '') : failed ? "We couldn't load the live summary just now." : 'Loading the summary…'}</div></div>
      ${c.objective && html`<p className="obj"><b>Educational objective.</b> ${c.objective}</p>`}
      <div className="acts"><button className="primary" onClick=${(e) => { e.stopPropagation(); onRead(); }}>Read the full case</button>
        ${data && data.url && html`<button onClick=${(e) => { e.stopPropagation(); toggleSaved({ title: c.wiki, extract: data.extract, thumb: data.thumb, url: data.url }); }}>${isSaved(saved, data.url) ? 'Saved ✓' : 'Save this page'}</button>`}
        ${data && data.url && html`<a href=${data.url} target="_blank" rel="noopener noreferrer" onClick=${(e) => e.stopPropagation()}>Wikipedia ↗</a>`}</div>
    </div>
  </div>`;
}

function CaseFiles({ pending, onRead }) {
  const [active, setActive] = useState(0);
  useEffect(() => { if (pending !== null && pending !== undefined) setActive(pending); }, [pending]);
  return html`<div className="files reveal">
    ${CASES.map((c) => html`<${CaseFile} key=${c.id} c=${c} active=${active === c.index} onActivate=${() => setActive(c.index)} onRead=${() => onRead(c)} />`)}
  </div>`;
}

function Reader({ c, onClose }) {
  useEffect(() => { const k = (e) => { if (e.key === 'Escape') onClose(); }; window.addEventListener('keydown', k); document.body.style.overflow = 'hidden'; return () => { window.removeEventListener('keydown', k); document.body.style.overflow = ''; }; }, [onClose]);
  const paras = c.exp.text.replace(/Educational objective:[\s\S]*$/, '').trim().split(/\n\n+/);
  return html`<div className="reader" onClick=${(e) => e.target === e.currentTarget && onClose()} role="dialog" aria-modal="true" aria-label=${c.title}>
    <div className="reader-card"><div className="reader-top"><span>${c.code} // ${c.title}</span><button onClick=${onClose} aria-label="Close"><${Icon} n="fa-xmark" /></button></div>
      <div className="reader-body two">
        <div><h4>Question</h4>${c.q.image && html`<img src=${c.q.image} alt="" style=${{ width: '100%', borderRadius: 14, marginBottom: 14, border: '1px solid rgba(35,35,26,.14)' }} />`}
          <p>${c.q.question}</p>${c.q.options.map((o, i) => html`<div key=${i} className=${cx('opt', i === c.q.answer && 'ok')}>${i === c.q.answer ? '✓ ' : ''}${o}</div>`)}</div>
        <div><h4>Explanation</h4>${c.exp.images.length > 0 && html`<div className="imgs">${c.exp.images.map((u, i) => html`<img key=${i} src=${u} alt=${`Explanation image ${i + 1}`} loading="lazy" />`)}</div>`}
          ${paras.map((p, i) => html`<p key=${i}>${p}</p>`)}${c.objective && html`<p className="obj"><b>Educational objective.</b> ${c.objective}</p>`}</div>
      </div></div></div>`;
}

/* ==========================================================================
   LIBRARY: Wikipedia explorer with stacked cards, bookmarks, notes and a reader
   ========================================================================== */
const SUGGEST = ['Finasteride', 'Benign prostatic hyperplasia', 'Testicular torsion', 'Brainstem stroke', 'Rheumatoid arthritis', 'Omeprazole', 'Parkinson disease'];
const SRC_TINT = { Wikipedia: '#3A2A20', PubMed: '#2C3B4A', 'Europe PMC': '#3F4A33' };

function useCardImage(page, topic) {
  const paper = page.source !== 'Wikipedia';
  const [img, setImg] = useState(page.thumb || '');
  const [kind, setKind] = useState(page.thumb ? 'own' : '');
  const ref = useRef(null);
  useEffect(() => {
    if (img || !paper || !ref.current) return undefined;
    let live = true;
    const go = () => {
      const fig = page.pmcid ? firstFigure(page.pmcid) : Promise.resolve('');
      fig.catch(() => '').then((u) => (u ? [u, 'figure'] : topic ? topicImage(topic).then((t) => [t, 'topic']) : ['', '']))
        .then(([u, k]) => { if (live && u) { setImg(u); setKind(k); } });
    };
    const io = 'IntersectionObserver' in window ? new IntersectionObserver((es) => { if (es.some((e) => e.isIntersecting)) { io.disconnect(); go(); } }, { rootMargin: '300px' }) : null;
    if (io) io.observe(ref.current); else go();
    return () => { live = false; if (io) io.disconnect(); };
  }, [page.url, topic]);
  return { img, kind, ref };
}

function WikiCard({ page: pg, saved, onRead, i, showNote, topic }) {
  const { img, kind, ref } = useCardImage(pg, topic);
  const page = img && !pg.thumb ? { ...pg, thumb: img } : pg;
  const paper = page.source !== 'Wikipedia';
  return html`<article className="wk" style=${{ '--i': i }}>
    <div className="wk-card">
      <div ref=${ref} className=${cx('wk-img', paper && !img && 'paper')} style=${paper ? { '--tint': SRC_TINT[page.source] || '#3A2A20' } : undefined}>
        ${page.thumb ? html`<img src=${page.thumb} alt="" loading="lazy" onError=${hidePicture} />` : paper ? html`<div className="ph"><span>${page.source}</span><b>${page.meta || 'Journal article'}</b></div>` : html`<div className="ph">${page.title.charAt(0)}</div>`}
        <span className="wk-src">${page.source}</span>${kind === 'topic' && html`<span className="wk-kind">Topic image</span>`}${kind === 'figure' && html`<span className="wk-kind">Figure 1</span>`}
        <button className=${cx('wk-save', saved && 'on')} onClick=${() => toggleSaved(page)} aria-pressed=${saved} aria-label=${saved ? 'Remove from saved' : 'Save this page'} title=${saved ? 'Saved' : 'Save for later'}><${Icon} n="fa-bookmark" cls=${saved ? 'fas' : 'far'} /></button>
      </div>
      <div className="wk-body"><h4>${page.title}</h4>${paper && html`<div className="wk-meta">${page.meta}${page.authors ? html`<br /><span>${page.authors}</span>` : ''}</div>`}<p>${page.extract}</p></div>
      ${showNote && html`<textarea className="wk-note" placeholder="A note to your future self…" value=${page.note || ''} onChange=${(e) => setSavedNote(page.url, e.target.value)} aria-label=${`Note on ${page.title}`}></textarea>`}
      <div className="wk-foot">
        <button className="read" onClick=${() => onRead(page)}>Read here</button>
        <a href=${page.url} target="_blank" rel="noopener noreferrer">${paper ? 'Original' : 'Wikipedia'} <${Icon} n="fa-arrow-up-right-from-square" /></a>
        <button className="ico" onClick=${() => copyText(page.url, 'Link copied')} aria-label="Copy link" title="Copy link"><${Icon} n="fa-link" /></button>
      </div>
    </div>
  </article>`;
}

/* One reader for every source: Wikipedia summaries, PubMed abstracts and open-access full text. */
function WikiReader({ page, onClose }) {
  const [blocks, setBlocks] = useState(null);
  const [state, setState] = useState('loading');
  const [hero, setHero] = useState(page.thumb || '');
  const saved = useSaved();
  const ann = annOf(useSyncExternalStore(AnnStore.subscribe, AnnStore.get), page.url);
  const [bar, setBar] = useState(null);
  const bodyRef = useRef(null);
  const downOnBg = useRef(false);
  const hls = hlList(ann);
  const [drawer, setDrawer] = useState(false);
  const [active, setActive] = useState('');
  const down = useRef(false);
  const pickSel = () => {
    const sel = window.getSelection(); const body = bodyRef.current;
    if (down.current) return;
    if (!sel || !sel.rangeCount || sel.isCollapsed || !body || !(body.contains(sel.anchorNode) || body.contains(sel.focusNode))) return setBar(null);
    const rg = sel.getRangeAt(0).cloneRange(); const br = document.createRange(); br.selectNodeContents(body);
    if (rg.compareBoundaryPoints(Range.START_TO_START, br) < 0) rg.setStart(br.startContainer, br.startOffset);
    if (rg.compareBoundaryPoints(Range.END_TO_END, br) > 0) rg.setEnd(br.endContainer, br.endOffset);
    const parts = Array.from(body.querySelectorAll('p')).filter((p) => rg.intersectsNode(p)).map((p) => {
      const pr = document.createRange(); pr.selectNodeContents(p);
      if (rg.compareBoundaryPoints(Range.START_TO_START, pr) > 0) pr.setStart(rg.startContainer, rg.startOffset);
      if (rg.compareBoundaryPoints(Range.END_TO_END, pr) < 0) pr.setEnd(rg.endContainer, rg.endOffset);
      return pr.toString().replace(/\s+/g, ' ').trim().slice(0, 400);
    }).filter((x) => x.length >= 3);
    if (!parts.length) return setBar(null);
    const r = sel.getRangeAt(0).getBoundingClientRect();
    const x = Math.round(Math.min(Math.max(r.left + r.width / 2, 150), window.innerWidth - 150) / 4) * 4;
    const y = Math.round(Math.min(Math.max(r.top - 12, 74), window.innerHeight - 70) / 4) * 4;
    setBar((o) => (o && o.t.join('|') === parts.join('|') && o.x === x && o.y === y ? o : { t: parts, x, y }));
  };
  useEffect(() => {
    let tm = 0; const on = () => { clearTimeout(tm); tm = setTimeout(pickSel, 240); };
    const dn = (e) => { if (e.target.closest && e.target.closest('.hl-bar')) return; down.current = true; setBar(null); };
    const up = () => { if (!down.current) return; down.current = false; clearTimeout(tm); tm = setTimeout(pickSel, 260); };
    document.addEventListener('selectionchange', on); document.addEventListener('mousedown', dn); document.addEventListener('mouseup', up);
    return () => { document.removeEventListener('selectionchange', on); document.removeEventListener('mousedown', dn); document.removeEventListener('mouseup', up); clearTimeout(tm); };
  });
  const addHl = (color, withNote) => {
    if (!bar) return;
    const fresh = bar.t.filter((x) => !hls.some((h) => h.t === x)).map((t) => ({ t, c: color, n: '' }));
    setAnn(page.url, { hl: [...hls, ...fresh], title: page.title });
    window.getSelection().removeAllRanges(); setBar(null);
    if (withNote) { setDrawer(true); setActive(bar.t[0]); }
  };
  const patchHl = (t, patch) => setAnn(page.url, { hl: hls.map((h) => (h.t === t ? { ...h, ...patch } : h)) });
  const delHl = (t) => setAnn(page.url, { hl: hls.filter((h) => h.t !== t) });
  const clickBody = (e) => { const h = e.target.closest && e.target.closest('mark.hlm'); if (h && window.getSelection().isCollapsed) { setActive(h.dataset.h); setDrawer(true); } };
  const jump = (t) => { setActive(t); const m = Array.from(bodyRef.current.querySelectorAll('mark.hlm')).find((x) => x.dataset.h === t); if (m) m.scrollIntoView({ behavior: 'smooth', block: 'center' }); };
  const copyNotes = () => copyText([`${page.title} (${page.source})`, page.url, '', ...hls.map((h) => `• “${h.t}”${h.n ? '\n   ' + h.n : ''}`), ann.note ? '\n' + ann.note : ''].join('\n'), 'Notes copied');
  useEffect(() => {
    const k = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', k); document.body.style.overflow = 'hidden';
    return () => { window.removeEventListener('keydown', k); document.body.style.overflow = ''; };
  }, [onClose]);
  useEffect(() => {
    let live = true;
    const abstractBlocks = () => page.extract.split(/\n+/).map((p) => ({ p }));
    if (page.pmcid) {
      paperFull(page.pmcid).then((b) => { if (!live) return; if (b.length) { setBlocks([{ h: 'Abstract', d: 0 }, ...abstractBlocks(), ...b]); setState('full'); } else { setBlocks(abstractBlocks()); setState('abstract'); } })
        .catch(() => { if (live) { setBlocks(abstractBlocks()); setState('abstract'); } });
    } else if (page.source === 'Wikipedia') {
      wikiFull(page.title).then((d) => { if (!live) return; if (d.thumb) setHero(d.thumb); setBlocks((d.extract || page.extract).split(/\n+/).map((p) => ({ p }))); setState('full'); })
        .catch(() => { if (live) { setBlocks(abstractBlocks()); setState('full'); } });
    } else { setBlocks(abstractBlocks()); setState('abstract'); }
    return () => { live = false; };
  }, [page.url]);
  const on = isSaved(saved, page.url);
  const paper = page.source !== 'Wikipedia';
  return ReactDOM.createPortal(html`<div className="reader" onMouseDown=${(e) => { downOnBg.current = e.target === e.currentTarget; }} onClick=${(e) => { if (e.target === e.currentTarget && downOnBg.current) onClose(); }} role="dialog" aria-modal="true" aria-label=${page.title}>
    <div className="reader-card narrow"><div className="reader-top"><span>${page.source} · reading here</span><div className="rt-r"><button className=${cx('rt-notes', drawer && 'on')} onClick=${() => setDrawer((d) => !d)}><${Icon} n="fa-pen-to-square" />Notes${(hls.length || ann.note) ? html`<i>${hls.length || '•'}</i>` : ''}</button><button onClick=${onClose} aria-label="Close"><${Icon} n="fa-xmark" /></button></div></div>
      <div className="reader-body" ref=${bodyRef} onClick=${clickBody} onScroll=${pickSel}>
        <div>
          ${hero && html`<img className="wk-hero" src=${hero} alt="" onError=${hidePicture} />`}
          <h3>${page.title}</h3>
          ${paper && html`<p className="rd-meta">${page.meta}${page.authors ? ' — ' + page.authors : ''}</p>`}
          ${state === 'full' && page.pmcid && html`<p className="rd-badge">Open access · full text</p>`}
          ${state === 'abstract' && paper && html`<p className="rd-badge soft">Abstract only. The full paper may be behind a paywall, so open the original to read it all.</p>`}
          ${!blocks && html`<p className="rd-load">Opening the article…</p>`}
          ${blocks && blocks.map((b, k) => b.img ? html`<figure key=${k} className="rd-fig"><img src=${b.img} alt=${b.cap} loading="lazy" onError=${(e) => { e.currentTarget.parentNode.style.display = 'none'; }} /><figcaption>${b.cap}</figcaption></figure>` : b.h ? html`<h4 key=${k} className=${cx('rd-h', b.d > 0 && 'sub')}>${b.h}</h4>` : html`<p key=${k}>${withHighlights(b.p, hls, active)}</p>`)}
          <div className="rd-acts">
            <button className=${on ? 'primary' : ''} onClick=${() => toggleSaved(page)}><${Icon} n="fa-bookmark" cls=${on ? 'fas' : 'far'} />${on ? 'Saved' : 'Save for later'}</button>
            <a href=${page.url} target="_blank" rel="noopener noreferrer">Open the original <${Icon} n="fa-arrow-up-right-from-square" /></a>
            <button onClick=${() => copyText(page.url, 'Link copied')}><${Icon} n="fa-link" />Copy link</button>
          </div>
        </div>
      </div></div></div>${bar && ReactDOM.createPortal(html`<div className="hl-bar" style=${{ left: bar.x, top: bar.y }} onMouseDown=${(e) => e.preventDefault()}>
        ${Object.entries(HL_COLORS).map(([k, c]) => html`<button key=${k} className="dot" style=${{ '--c': c }} onClick=${() => addHl(k, false)} aria-label=${'Highlight ' + k} title="Highlight"></button>`)}
        <span className="sep"></span><button className="nbtn" onClick=${() => addHl('y', true)}><${Icon} n="fa-pen" />Add note</button></div>`, document.body)}
      ${ReactDOM.createPortal(html`<aside className=${cx('rd-drawer', drawer && 'open')} aria-hidden=${!drawer}>
        <div className="rdd-top"><b>Your notes</b><span>${hls.length} highlight${hls.length === 1 ? '' : 's'}</span><button onClick=${copyNotes} title="Copy all notes" aria-label="Copy notes"><${Icon} n="fa-copy" /></button><button onClick=${() => setDrawer(false)} aria-label="Close notes"><${Icon} n="fa-xmark" /></button></div>
        <div className="rdd-list">
          ${hls.length === 0 && html`<p className="rd-hint">Select any text in the article. Pick a colour to highlight it, or choose Add note to write about it.</p>`}
          ${hls.map((h) => html`<div key=${h.t} className=${cx('rdd-card', active === h.t && 'active')} style=${{ '--c': HL_COLORS[h.c] || HL_COLORS.y }}>
            <button className="q" onClick=${() => jump(h.t)}>${h.t}</button>
            <textarea value=${h.n} placeholder="Add a note…" onFocus=${() => setActive(h.t)} onChange=${(e) => patchHl(h.t, { n: e.target.value })} ref=${(el) => { if (el && active === h.t && drawer && !h.n && document.activeElement !== el && !el.dataset.f) { el.dataset.f = 1; setTimeout(() => el.focus({ preventScroll: true }), 320); } }}></textarea>
            <div className="row">${Object.entries(HL_COLORS).map(([k, c]) => html`<button key=${k} className=${cx('dot', h.c === k && 'on')} style=${{ '--c': c }} onClick=${() => patchHl(h.t, { c: k })} aria-label=${'Colour ' + k}></button>`)}<button className="mkc" onClick=${() => addCard(h.n || 'Recall: ' + page.title, h.t, page.title)} title="Make a flashcard" aria-label="Make a flashcard"><${Icon} n="fa-clone" /></button><button className="del" onClick=${() => delHl(h.t)} aria-label="Delete highlight"><${Icon} n="fa-trash-can" /></button></div>
          </div>`)}
          <label className="rdd-gen"><span>About this page</span><textarea placeholder="Write your own notes on this page…" value=${ann.note} onChange=${(e) => { setAnn(page.url, { note: e.target.value }); if (isSaved(saved, page.url)) setSavedNote(page.url, e.target.value); }}></textarea></label>
        </div></aside>`, document.body)}`, document.body);
}

function Explorer() {
  const [q, setQ] = useState('');
  const [source, setSource] = useState('all');
  const [res, setRes] = useState([]);
  const [state, setState] = useState('idle');
  const [nonce, setNonce] = useState(0);
  const [reading, setReading] = useState(null);
  const tab = useSyncExternalStore(LibTab.subscribe, LibTab.get);
  const saved = useSaved();

  useEffect(() => {
    if (q.trim().length < 2) { setRes([]); setState('idle'); return undefined; }
    const ctl = new AbortController();
    setState('loading');
    const t = setTimeout(() => {
      searchSources(q, source, ctl.signal, 0, (partial) => { if (!ctl.signal.aborted && partial.length) { setRes(partial); setState('ok'); } })
        .then((r) => { if (ctl.signal.aborted) return; setRes(r); setState(r.length ? 'ok' : 'none'); })
        .catch((e) => { if (e.name !== 'AbortError') setState('error'); });
    }, 380);
    return () => { clearTimeout(t); ctl.abort(); };
  }, [q, source, nonce]);

  const pick = (term) => { LibTab.set('results'); setQ(term); };
  const exportList = () => copyText(saved.map((p) => `• ${p.title} (${p.source || 'Wikipedia'}) — ${p.url}${p.note ? `\n   ${p.note}` : ''}`).join('\n'), 'Your list is copied');
  const clearAll = () => { if (confirm('Remove every saved page?')) { SavedStore.set([]); toast('Cleared'); } };

  return html`<div className="explorer">
    <div className="ex-head reveal"><p className="eyebrow">Go deeper</p><h2>Curious? <span className="hl">Read it here.</span></h2>
      <p>Search Wikipedia, PubMed and open-access journals in one place. Open anything right here, save what you love, and leave yourself a note.</p></div>
    <label className="ex-input frost reveal" style=${{ '--d': '80ms' }}><${Icon} n="fa-magnifying-glass" /><input value=${q} onChange=${(e) => { LibTab.set('results'); setQ(e.target.value); }} placeholder="Try “finasteride” or “brainstem stroke”…" aria-label="Search articles and papers" />
      ${q && html`<button className="ex-x" onClick=${() => setQ('')} aria-label="Clear"><${Icon} n="fa-xmark" /></button>`}</label>
    <div className="ex-row">
      <${Seg} value=${source} onChange=${setSource} options=${SOURCES} />
      <${Seg} value=${tab} onChange=${(v) => LibTab.set(v)} options=${[['results', 'Results'], ['saved', `Saved${saved.length ? ' · ' + saved.length : ''}`]]} />
    </div>
    <div className="ex-chips">${SUGGEST.map((t) => html`<button key=${t} onClick=${() => pick(t)}>${t}</button>`)}</div>

    ${tab === 'results' && html`<${Fragment}>
      ${state === 'idle' && html`<p className="ex-msg">Type a topic above, or tap one of the ideas. Journal papers show the abstract, and open-access ones can be read in full without leaving this page.</p>`}
      ${state === 'none' && html`<p className="ex-msg">We couldn't find anything for “${q}” in ${source === 'all' ? 'these sources' : SOURCES.find((s) => s[0] === source)[1]}. A simpler word, or another source, might help.</p>`}
      ${state === 'error' && html`<p className="ex-msg">We couldn't reach the library just now. Check your connection, then <button className="btn-ghost btn-sm" onClick=${() => setNonce((n) => n + 1)}>try again</button></p>`}
      <div className="ex-grid">
        ${state === 'loading' && [0, 1, 2, 3, 4, 5].map((k) => html`<div key=${k} className="wk sk"><div className="wk-card"></div></div>`)}
        ${state !== 'loading' && res.map((p, i) => html`<${WikiCard} key=${p.url} page=${p} i=${i} saved=${isSaved(saved, p.url)} topic=${q.trim()} onRead=${setReading} />`)}
      </div><//>`}

    ${tab === 'saved' && html`<${Fragment}>
      ${saved.length > 0 && html`<div className="ex-tools"><button className="btn-ghost btn-sm" onClick=${exportList}><${Icon} n="fa-copy" />Copy my list</button><button className="btn-ghost btn-sm" onClick=${clearAll}><${Icon} n="fa-trash-can" />Clear all</button></div>`}
      ${saved.length === 0 && html`<p className="ex-msg">Nothing saved yet. Tap the bookmark on any card and it will wait for you right here.</p>`}
      <div className="ex-grid">${saved.map((p, i) => html`<${WikiCard} key=${p.url} page=${{ source: 'Wikipedia', ...p }} i=${i} saved=${true} showNote=${true} onRead=${setReading} />`)}</div><//>`}

    ${reading && html`<${WikiReader} page=${{ source: 'Wikipedia', ...reading }} onClose=${() => setReading(null)} />`}
  </div>`;
}

const SYS_RAW = [
 {
  "k": "bd",
  "name": "Whole body",
  "sub": "BioDigital · labels, layers, search",
  "ico": "fa-person",
  "src": "https://human.biodigital.com/viewer/?id=5HPq&ui-anatomy-descriptions=true&ui-anatomy-pronunciations=true&ui-anatomy-labels=true&ui-audio=true&ui-chapter-list=false&ui-fullscreen=true&ui-help=true&ui-info=true&ui-label-list=true&ui-layers=true&ui-loader=circle&ui-media-controls=full&ui-menu=true&ui-nav=true&ui-search=true&ui-tools=true&ui-tutorial=false&ui-undo=true&ui-whiteboard=true&initial.none=true&disable-scroll=false&dk=edac91d2443ff104ab74174b3047b00937a21960&paid=o_25ed890c",
  "credit": "BioDigital Human"
 },
 {
  "k": "sf",
  "name": "Skeleton",
  "sub": "Every bone, rotate and zoom",
  "ico": "fa-bone",
  "src": "4de7b96a351a4a35b1b6e5415277ff07",
  "thumb": "https://media.sketchfab.com/models/4de7b96a351a4a35b1b6e5415277ff07/thumbnails/5fddaf2e9e7242b5ad944a56f4b04b50/200x200.jpeg",
  "credit": "“Skeleton” by Diego Luján García, CC BY 4.0"
 },
 {
  "k": "sf",
  "name": "Muscles",
  "sub": "Muscular system, male",
  "ico": "fa-dumbbell",
  "src": "991eb96938be4d0d8fadee241a1063d3",
  "thumb": "https://media.sketchfab.com/models/991eb96938be4d0d8fadee241a1063d3/thumbnails/0fb3e432bec0433792ab048ecad40fbe/cb3a614029f24a5d8ba3b800015732cc.jpeg",
  "credit": "“Male Body Muscular System” by Ruslan Gadzhiev, CC BY 4.0"
 },
 {
  "k": "sf",
  "name": "Heart",
  "sub": "Chambers, valves and vessels",
  "ico": "fa-heart-pulse",
  "src": "3f8072336ce94d18b3d0d055a1ece089",
  "thumb": "https://media.sketchfab.com/models/3f8072336ce94d18b3d0d055a1ece089/thumbnails/021a0c163a16426b8b4d2c4244e8386d/fa4b143e70bf43b0984a146ebd068f6e.jpeg",
  "credit": "“Realistic Human Heart” by neshallads, CC BY 4.0"
 },
 {
  "k": "sf",
  "name": "Circulation",
  "sub": "Heart and blood vessels in the body",
  "ico": "fa-droplet",
  "src": "6a7a537a71444f6e8201e18a685a013d",
  "thumb": "https://media.sketchfab.com/models/6a7a537a71444f6e8201e18a685a013d/thumbnails/572778bb27ae4dcf8d444bb17aabf673/ae59434f41d441f0af8ddba9be4b3051.jpeg",
  "credit": "“Animated Human Body with circulatory system” by AVRcontent"
 },
 {
  "k": "sf",
  "name": "Lungs",
  "sub": "Respiratory system",
  "ico": "fa-lungs",
  "src": "250911151757489da1cf5501b791f363",
  "thumb": "https://media.sketchfab.com/models/250911151757489da1cf5501b791f363/thumbnails/3febed3ef5bd46439f7dc053fd02cc1d/c884b9c1c2834f41934db13144203c4a.jpeg",
  "credit": "“Human Respiratory system review” by Darmawan Pujisetyadi"
 },
 {
  "k": "sf",
  "name": "Digestive tract",
  "sub": "Gastrointestinal tract",
  "ico": "fa-utensils",
  "src": "8c4c69035f4f4773813533d7ca56e516",
  "thumb": "https://media.sketchfab.com/models/8c4c69035f4f4773813533d7ca56e516/thumbnails/d00e60e12688486eae3e7c766f441299/f88fa021a6fa4f29aa707e6b1f8b9a6f.jpeg",
  "credit": "“Gastrointestinal tract” by Ebers"
 },
 {
  "k": "sf",
  "name": "Brainstem and nerves",
  "sub": "Cranial nerves",
  "ico": "fa-brain",
  "src": "2792c4a8600b4984960c5caf949feb4d",
  "thumb": "https://media.sketchfab.com/models/2792c4a8600b4984960c5caf949feb4d/thumbnails/bfcd2ec2f59944b5ad0fca2e89aad1dc/6557370b8df444d48862d72b4b2a48fd.jpeg",
  "credit": "“Cranial nerves and brain stem” by Ebers"
 },
 {
  "k": "sf",
  "name": "Skull and neck",
  "sub": "Head and cervical spine",
  "ico": "fa-skull",
  "src": "a47ef69ff3a4402783cff0f841bc5e0a",
  "thumb": "https://media.sketchfab.com/models/a47ef69ff3a4402783cff0f841bc5e0a/thumbnails/0d6192e3499b422baf5fa0242afb7ba1/c3f7c299d6f9439f800ba96fc1026e36.jpeg",
  "credit": "“Human skull and neck” by Lassi Kaukonen, CC BY 4.0"
 },
 {
  "k": "sf",
  "name": "Animated body",
  "sub": "Full body with moving parts",
  "ico": "fa-child-reaching",
  "src": "9b0b079953b840bc9a13f524b60041e4",
  "thumb": "https://media.sketchfab.com/models/9b0b079953b840bc9a13f524b60041e4/thumbnails/7f730db44a9e4c60a2f1f6aabb7b0e32/281858f86fb64416b0037496aa4cbfa6.jpeg",
  "credit": "“Animated Full Human Body Anatomy” by AVRcontent"
 }
];
/* BioDigital only loads on websites registered with the developer key, so it sits last. */
const SYSTEMS_3D = [...SYS_RAW.slice(1), { ...SYS_RAW[0], name: 'BioDigital whole body', sub: 'Works on your registered website only' }];
function Anatomy3D() {
  const [i, setI] = useState(0);
  const [open, setOpen] = useState(false);
  const [ready, setReady] = useState(false);
  const wrap = useRef(null);
  const [armed, setArmed] = useState(false);
  const sys = SYSTEMS_3D[i];
  const url = sys.k === 'bd' ? sys.src : `https://sketchfab.com/models/${sys.src}/embed?autostart=1&ui_theme=dark&ui_hint=0&ui_infos=0&ui_stop=0&dnt=1`;
  useEffect(() => {
    const el = wrap.current; if (!el) return undefined;
    const io = 'IntersectionObserver' in window ? new IntersectionObserver((es) => { if (es.some((e) => e.isIntersecting)) { setArmed(true); io.disconnect(); } }, { rootMargin: '400px' }) : null;
    if (io) io.observe(el); else setArmed(true);
    return () => { if (io) io.disconnect(); };
  }, []);
  useEffect(() => {
    if (!open) return undefined;
    const k = (e) => { if (e.key === 'Escape') setOpen(false); };
    const c = (e) => { if (!e.target.closest('.a3-pick')) setOpen(false); };
    window.addEventListener('keydown', k); document.addEventListener('mousedown', c);
    return () => { window.removeEventListener('keydown', k); document.removeEventListener('mousedown', c); };
  }, [open]);
  const choose = (n) => { if (n !== i) { setReady(false); setI(n); } setOpen(false); };
  return html`<section className="a3 reveal" ref=${wrap}>
    <div className="a3-head">
      <div><p className="eyebrow">Explore in 3D</p><h3>Pick a system. <span className="hl">Turn it around.</span></h3></div>
      <div className=${cx('a3-pick', open && 'open')}>
        <button className="a3-btn" onClick=${() => setOpen((o) => !o)} aria-haspopup="listbox" aria-expanded=${open}>
          <span className="a3-ico"><${Icon} n=${sys.ico} /></span><span className="a3-txt"><small>System</small><b>${sys.name}</b></span><${Icon} n="fa-chevron-down" cls="fas a3-chev" />
        </button>
        <ul className="a3-menu" role="listbox" aria-hidden=${!open}>
          ${SYSTEMS_3D.map((s, n) => html`<li key=${n} role="option" aria-selected=${n === i}><button onClick=${() => choose(n)} className=${n === i ? 'on' : ''} tabIndex=${open ? 0 : -1}>
            <span className="a3-thumb">${s.thumb ? html`<img src=${s.thumb} alt="" loading="lazy" onError=${hidePicture} />` : html`<${Icon} n=${s.ico} />`}</span>
            <span className="a3-li"><b>${s.name}</b><small>${s.sub}</small></span>${n === i && html`<${Icon} n="fa-check" />`}</button></li>`)}
        </ul>
      </div>
    </div>
    <div className="a3-frame">
      ${!ready && html`<div className="a3-load"><span></span><p>Loading the ${sys.name.toLowerCase()} model…</p></div>`}
      ${armed && html`<iframe key=${url} src=${url} title=${sys.name + ' 3D model'} scrolling="no" allow="autoplay; fullscreen; xr-spatial-tracking" allowFullScreen onLoad=${() => setReady(true)}></iframe>`}
    </div>
    <p className="a3-credit">${sys.credit}${sys.k === 'sf' ? ' · via Sketchfab' : ''}. Drag to rotate, scroll to zoom. <a href=${url} target="_blank" rel="noopener noreferrer">Open full screen ↗</a></p>
  </section>`;
}

const MONA = 'https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@900e7160c25e1f6ae0ddd82cf67d941d375242d7/usmle-quiz/assets/art/mona-lisa.jpg';
/* Yellow Library opener: a slowly turning asterisk reveals and hides the Mona Lisa behind it. */
function LibHero() {
  const spin = !reduceMotion();
  const bars = [[200, 0], [150, 60], [170, 120]];
  const go = () => { const el = document.getElementById('lib-cases'); if (el) el.scrollIntoView({ behavior: 'smooth' }); };
  return html`<section className="lib-hero" data-nav="light">
    <div className="lh-fan" aria-hidden="true">
      <svg viewBox="0 0 1000 1000" role="img" aria-label="The Mona Lisa, revealed by a turning fan">
        <defs><mask id="fanm" maskUnits="userSpaceOnUse" x="-300" y="-300" width="1600" height="1600">
          <rect x="-300" y="-300" width="1600" height="1600" fill="#000" />
          <g fill="#fff">${bars.map(([w, a]) => html`<rect key=${a} x=${500 - w / 2} y="-100" width=${w} height="1200" transform=${'rotate(' + a + ' 500 500)'} />`)}
            ${spin && html`<animateTransform attributeName="transform" type="rotate" from="0 500 500" to="360 500 500" dur="64s" repeatCount="indefinite" />`}</g>
        </mask></defs>
        <image href=${MONA} x="-100" y="-90" width="1200" height="1789" preserveAspectRatio="xMidYMid slice" mask="url(#fanm)" />
      </svg>
    </div>
    <div className="lh-copy">
      <h1>Dig into the <span className="lh-u">why<svg viewBox="0 0 220 24" preserveAspectRatio="none" aria-hidden="true"><path d="M3,15 C40,6 70,20 112,12 S180,8 217,14" /></svg></span> behind every answer</h1>
      <p>Case files, Wikipedia, PubMed and open-access journals, together in one quiet place. Search, highlight and keep your own notes.</p>
      <button className="lh-btn" onClick=${go}>Browse the cases <${Icon} n="fa-arrow-down" /></button>
    </div>
  </section>`;
}

function CasesSection({ pending, onRead, withExplorer }) {
  return html`<section className=${cx('sec', withExplorer && 'lib')} id=${withExplorer ? 'lib-cases' : undefined} data-tone="paper" data-nav="light" style=${withExplorer ? { minHeight: '100dvh' } : undefined}>
    <div className="wrap">
      <h2 className="files-h reveal" style=${{ '--d': '80ms', marginTop: 18 }}>Open a case. <span className="hl">Read the story</span> behind the answer.</h2>
      <${CaseFiles} pending=${pending} onRead=${onRead} />
      ${withExplorer && html`<${Fragment}><${Anatomy3D} /><${Explorer} /><//>`}
    </div>
  </section>`;
}

/* ==========================================================================
   FOOTER  (Caravaggio, Supper at Emmaus as the backdrop)
   ========================================================================== */
const BL = (t) => String(t).split('').map((c, i) => html`<span key=${i} style=${{ '--i': i }} aria-hidden="true">${c}</span>`);
function Footer({ go, openFind }) {
  const link = (r, label) => html`<button className="flink bl" aria-label=${label} onClick=${() => go(r)}>${BL(label)}</button>`;
  return html`<footer className="site-footer" data-nav="dark" style=${{ '--poster': `url(${CARAV.poster})` }}>
    <div className="sf-bg" aria-hidden="true"></div>
    <div className="sf-inner">
      <div className="cols">
        <div><div style=${{ display: 'flex', alignItems: 'center', gap: 10, fontWeight: 700, fontSize: 17 }}><span className="brand-mark"><i className="fas fa-plus"></i></span>USMLE Quiz</div>
          <p className="blurb">A free place to practise for Step 1, made with care for everyone on the long road to a white coat. You've got this.</p>
          <button className="btn-white" onClick=${() => go('quiz')}>Start a quiz <${Icon} n="fa-arrow-right" /></button></div>
        <div><h5>Wander</h5>${link('home', 'Home')}${link('quiz', 'Quiz')}${link('dashboard', 'Dashboard')}${link('study', 'Study')}${link('library', 'Library')}${link('community', 'Community')}</div>
        <div><h5>Little helpers</h5><button className="flink bl" aria-label="Find anything" onClick=${openFind}>${BL('Find anything · ⌘K')}</button>${link('quiz', 'Calculator')}${link('quiz', 'Study playlist')}${link('dashboard', 'Focus timer')}</div>
        <div><h5>Read more</h5>${CASES.slice(0, 5).map((c) => html`<a key=${c.id} href=${'https://en.wikipedia.org/wiki/' + encodeURIComponent(c.wiki.replace(/ /g, '_'))} target="_blank" rel="noopener noreferrer" className="bl" aria-label=${c.wiki}>${BL(c.wiki + ' ↗')}</a>`)}</div>
      </div>
      <div className="footer-bar"><span>© ${new Date().getFullYear()} USMLE Quiz · Free, and staying that way. A practice tool, not a substitute for your curriculum.<br />Backdrop: Caravaggio, 1571–1610.</span>
        <button className="totop" onClick=${() => window.scrollTo({ top: 0, behavior: 'smooth' })} aria-label="Back to top"><${Icon} n="fa-arrow-up" /></button></div>
    </div>
  </footer>`;
}

/* ==========================================================================
   FIND  (⌘K / "/")  local questions + live Wikipedia
   ========================================================================== */
function highlight(text, tokens) {
  if (!tokens.length) return text;
  const re = new RegExp('(' + tokens.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|') + ')', 'ig');
  return text.split(re).map((part, i) => (i % 2 ? html`<mark key=${i}>${part}</mark>` : part));
}
function snippet(text, tokens) {
  const flat = text.replace(/\s+/g, ' ');
  const low = flat.toLowerCase();
  let at = -1;
  for (const t of tokens) { const i = low.indexOf(t); if (i >= 0 && (at < 0 || i < at)) at = i; }
  if (at < 0) return flat.slice(0, 120);
  const s = Math.max(0, at - 50);
  return (s ? '…' : '') + flat.slice(s, s + 150) + '…';
}

function Palette({ onClose, go, openCase }) {
  const [q, setQ] = useState('');
  const [sel, setSel] = useState(0);
  const [wiki, setWiki] = useState([]);
  const [busy, setBusy] = useState(false);
  const inputRef = useRef(null);
  useEffect(() => { inputRef.current && inputRef.current.focus(); }, []);
  const tokens = q.toLowerCase().split(/\s+/).filter(Boolean);

  const actions = useMemo(() => [
    { id: 'a1', ic: 'fa-pen-to-square', t: 'Start the quiz', s: '30:00 timed run', run: () => go('quiz') },
    { id: 'a2', ic: 'fa-chart-simple', t: 'Open the dashboard', s: 'Scores, streak and history', run: () => go('dashboard') },
    { id: 'a3', ic: 'fa-folder-open', t: 'Browse case files', s: 'Explanations and Wikipedia', run: () => go('library') },
    { id: 'a4', ic: 'fa-house', t: 'Go home', s: '', run: () => go('home') },
  ].filter(a => !tokens.length || tokens.every(t => (a.t + ' ' + a.s).toLowerCase().includes(t))), [q]);

  const cases = useMemo(() => CASES.map(c => {
    const hay = (c.title + ' ' + c.q.question + ' ' + c.q.options.join(' ') + ' ' + c.exp.text);
    if (tokens.length && !tokens.every(t => hay.toLowerCase().includes(t))) return null;
    return { id: c.id, ic: 'fa-file-medical', t: `${c.code} · ${c.title}`, s: tokens.length ? snippet(hay.slice(c.title.length), tokens) : c.tag, run: () => { openCase(c.index); go('library'); } };
  }).filter(Boolean), [q]);

  const savedHits = useMemo(() => SavedStore.get().filter((p) => !tokens.length || tokens.every((t) => (p.title + ' ' + (p.note || '')).toLowerCase().includes(t)))
    .slice(0, 5).map((p) => ({ id: 's' + p.url, ic: 'fa-bookmark', img: p.thumb, t: p.title, s: p.note || p.extract, run: () => window.open(p.url, '_blank', 'noopener') })), [q]);

  useEffect(() => {
    if (q.trim().length < 3) { setWiki([]); setBusy(false); return undefined; }
    const ctl = new AbortController();
    setBusy(true);
    const t = setTimeout(() => { searchSources(q, 'all', ctl.signal, 2, (p) => { if (!ctl.signal.aborted) setWiki(p); }).then(r => { setWiki(r); setBusy(false); }).catch(e => { if (e.name !== 'AbortError') setBusy(false); }); }, 380);
    return () => { clearTimeout(t); ctl.abort(); };
  }, [q]);

  const wikiItems = wiki.map(w => ({ id: 'w' + w.url, ic: w.source === 'Wikipedia' ? 'fa-globe' : 'fa-file-lines', img: w.thumb, t: w.title, s: w.source + ' · ' + w.extract, run: () => window.open(w.url, '_blank', 'noopener') }));
  const groups = [['Go to', actions], ['Case files', cases], ['Your saved pages', savedHits], ['Articles and papers', wikiItems]].filter(g => g[1].length);
  const flat = groups.flatMap(g => g[1]);
  useEffect(() => { setSel(0); }, [q, wiki.length]);

  const choose = (item) => { if (!item) return; onClose(); item.run(); };
  const onKey = (e) => {
    if (e.key === 'Escape') { e.preventDefault(); onClose(); }
    else if (e.key === 'ArrowDown') { e.preventDefault(); setSel(s => Math.min(flat.length - 1, s + 1)); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setSel(s => Math.max(0, s - 1)); }
    else if (e.key === 'Enter') { e.preventDefault(); choose(flat[sel]); }
  };
  useEffect(() => { const el = document.querySelector('.pal-item.sel'); if (el && el.scrollIntoView) el.scrollIntoView({ block: 'nearest' }); }, [sel]);

  let idx = -1;
  return html`<div className="pal-back" onMouseDown=${(e) => e.target === e.currentTarget && onClose()}>
    <div className="pal" role="dialog" aria-modal="true" aria-label="Find" onKeyDown=${onKey}>
      <div className="pal-in"><${Icon} n="fa-magnifying-glass" /><input ref=${inputRef} value=${q} onChange=${(e) => setQ(e.target.value)} placeholder="Find questions, explanations, topics…" aria-label="Find" />${busy && html`<i className="fas fa-circle-notch fa-spin" style=${{ opacity: .6 }}></i>`}</div>
      <div className="pal-list">
        ${!flat.length && html`<p style=${{ padding: 24, color: 'rgba(255,255,255,.55)', fontSize: 14 }}>${busy ? 'Searching Wikipedia…' : 'No matches yet. Keep typing.'}</p>`}
        ${groups.map(([name, items]) => html`<div key=${name}><div className="pal-grp">${name}</div>
          ${items.map(it => { idx++; const me = idx; return html`<button key=${it.id} className=${cx('pal-item', sel === me && 'sel')} onMouseEnter=${() => setSel(me)} onClick=${() => choose(it)}>
            <span className="ic">${it.img ? html`<img src=${it.img} alt="" />` : html`<${Icon} n=${it.ic} />`}</span>
            <span><b>${highlight(it.t, tokens)}</b>${it.s && html`<small>${name === 'Case files' && tokens.length ? highlight(it.s, tokens) : it.s}</small>`}</span>
            <${Icon} n="fa-arrow-turn-down" cls="rot" /></button>`; })}</div>`)}
      </div>
      <div className="pal-foot"><span><kbd>↑↓</kbd>navigate</span><span><kbd>↵</kbd>open</span><span><kbd>esc</kbd>close</span></div>
    </div>
  </div>`;
}

/* Is the page behind this point light or dark? (skips the nav itself) */
function toneAt(x, y) {
  for (const el of document.elementsFromPoint(x, y)) {
    if (el.closest('.site-nav, .tabs, .stack-btn, .acct-wrap, .toasts, .progress')) continue;
    if (el.tagName === 'IMG' || el.closest('.has-art, .fc-art, .qimage')) return 'dark';
    const m = getComputedStyle(el).backgroundColor.match(/rgba?\(([^)]+)\)/);
    if (!m) continue;
    const [r, g, b, a = 1] = m[1].split(',').map(parseFloat);
    if (a < 0.5) continue;
    return 0.2126 * r + 0.7152 * g + 0.0722 * b > 128 ? 'light' : 'dark';
  }
  return 'dark';
}

/* ==========================================================================
   STUDY: notes with a sketch pad, spaced-repetition cards, mistake notebook,
   planner and a pomodoro timer. Everything is saved on this device.
   ========================================================================== */
const NotesStore = makeStore('usmle-site:notes', []);
const CardStore = makeStore('usmle-site:cards', []);
const PlanStore = makeStore('usmle-site:plan', { exam: '', goal: 20, tasks: [] });
const FocusLog = makeStore('usmle-site:focus', []);                 /* [{ at, sec }] finished focus blocks */
const MasteredStore = makeStore('usmle-site:mastered', []);
const GoalsStore = makeStore('usmle-site:goals', { accuracy: 75, weeklyHours: 20, why: '', milestones: [] });
const PrefsStore = makeStore('usmle-site:prefs', { sound: true, collapseNav: true, reduceMotion: false, landing: 'home', globe: true });
const useStore = (st) => useSyncExternalStore(st.subscribe, st.get);
const uid = () => Math.random().toString(36).slice(2, 9) + Date.now().toString(36).slice(-3);
const DAY = 864e5;
const logFocus = (sec) => { if (sec >= 30) FocusLog.set([...FocusLog.get(), { at: Date.now(), sec }].slice(-500)); };
const focusToday = (log) => Math.round(log.filter((f) => dayKey(f.at) === new Date().toDateString()).reduce((s, f) => s + f.sec, 0) / 60);

function addCard(front, back, src) {
  const cards = CardStore.get();
  if (cards.some((c) => c.front === front && c.back === back)) { toast('That card is already in your deck'); return false; }
  CardStore.set([{ id: uid(), front, back, src: src || '', due: Date.now(), ivl: 0, ease: 2.4, reps: 0, created: Date.now() }, ...cards]);
  toast('Added to your flashcards'); return true;
}

/* ---------- sketch pad ---------- */
const SK_W = 1000, SK_H = 620;
const SK_COLORS = ['#111111', '#E5383B', '#2B6CFF', '#16A34A', '#F5A300'];
function Sketch({ note, onInk }) {
  const bgRef = useRef(null), inkRef = useRef(null), fileRef = useRef(null);
  const [tool, setTool] = useState('pen'), [color, setColor] = useState(SK_COLORS[0]), [size, setSize] = useState(4);
  const undo = useRef([]);
  const bgData = useRef(note.bg || '');
  const bgRefVal = useRef(note.bgRef || '');
  const [hasBg, setHasBg] = useState(!!(note.bg || note.bgRef));
  const loadImg = (src, cross) => new Promise((ok, no) => { const im = new Image(); if (cross) im.crossOrigin = 'anonymous'; im.onload = () => ok(im); im.onerror = no; im.src = src; });
  const paintBg = (im) => {
    const c = bgRef.current.getContext('2d'); c.fillStyle = '#fff'; c.fillRect(0, 0, SK_W, SK_H);
    if (im) { const k = Math.min(SK_W / im.width, SK_H / im.height); const w = im.width * k, h = im.height * k; c.drawImage(im, (SK_W - w) / 2, (SK_H - h) / 2, w, h); }
  };
  useEffect(() => {
    paintBg(null);
    if (note.bgRef) loadImg(note.bgRef, true).then(paintBg).catch(() => {});
    else if (note.bg) loadImg(note.bg).then(paintBg).catch(() => {});
    if (note.ink) loadImg(note.ink).then((im) => inkRef.current.getContext('2d').drawImage(im, 0, 0)).catch(() => {});
  }, []);
  const save = () => onInk({ ink: inkRef.current.toDataURL('image/webp', 0.8), bg: bgData.current, bgRef: bgRefVal.current });
  const setBackground = async (src, cross) => {
    try {
      const im = await loadImg(src, cross); paintBg(im);
      if (cross) { bgData.current = ''; bgRefVal.current = src; } else { bgData.current = bgRef.current.toDataURL('image/jpeg', 0.65); bgRefVal.current = ''; }   /* picture presets are saved as a link, not a copy */
      setHasBg(true); save();
    } catch { toast('That image could not be loaded'); }
  };
  const pos = (e) => { const r = inkRef.current.getBoundingClientRect(); return [(e.clientX - r.left) * SK_W / r.width, (e.clientY - r.top) * SK_H / r.height]; };
  const stroke = useRef(null);
  const down = (e) => {
    e.preventDefault(); inkRef.current.setPointerCapture(e.pointerId);
    const ctx = inkRef.current.getContext('2d');
    undo.current = [...undo.current.slice(-19), ctx.getImageData(0, 0, SK_W, SK_H)];
    const [x, y] = pos(e); stroke.current = { pts: [[x, y]], snap: undo.current[undo.current.length - 1] };
    draw(e, true);
  };
  const style = (ctx, p) => {
    ctx.lineCap = 'round'; ctx.lineJoin = 'round';
    if (tool === 'eraser') { ctx.globalCompositeOperation = 'destination-out'; ctx.globalAlpha = 1; ctx.lineWidth = size * 5; }
    else if (tool === 'hl') { ctx.globalCompositeOperation = 'source-over'; ctx.globalAlpha = 0.35; ctx.strokeStyle = color; ctx.lineWidth = size * 5; }
    else { ctx.globalCompositeOperation = 'source-over'; ctx.globalAlpha = 1; ctx.strokeStyle = color; ctx.lineWidth = Math.max(1, size * (0.6 + (p || 0.5) * 0.9)); }
  };
  const draw = (e, first) => {
    const s = stroke.current; if (!s) return;
    const ctx = inkRef.current.getContext('2d'); const [x, y] = pos(e);
    if (!first) s.pts.push([x, y]);
    if (tool === 'hl') {                          /* redraw the whole stroke so overlaps do not darken */
      ctx.putImageData(s.snap, 0, 0); style(ctx);
      ctx.beginPath(); s.pts.forEach(([px, py], i) => (i ? ctx.lineTo(px, py) : ctx.moveTo(px, py))); if (s.pts.length === 1) ctx.lineTo(x + 0.01, y); ctx.stroke();
    } else {
      style(ctx, e.pressure); const n = s.pts.length; const a = s.pts[n - 2] || s.pts[n - 1], b = s.pts[n - 1];
      ctx.beginPath(); ctx.moveTo(a[0], a[1]); ctx.lineTo(b[0] + (n === 1 ? 0.01 : 0), b[1]); ctx.stroke();
    }
    ctx.globalAlpha = 1; ctx.globalCompositeOperation = 'source-over';
  };
  const move = (e) => { if (stroke.current) draw(e, false); };
  const up = () => { if (!stroke.current) return; stroke.current = null; save(); };
  const undoOne = () => { const snap = undo.current.pop(); if (!snap) return; inkRef.current.getContext('2d').putImageData(snap, 0, 0); save(); };
  const clearAll = () => { if (!confirm('Clear the drawing and the background?')) return; undo.current = []; inkRef.current.getContext('2d').clearRect(0, 0, SK_W, SK_H); paintBg(null); bgData.current = ''; bgRefVal.current = ''; setHasBg(false); save(); };
  const exportPng = () => {
    const c = document.createElement('canvas'); c.width = SK_W; c.height = SK_H; const x = c.getContext('2d');
    x.drawImage(bgRef.current, 0, 0); x.drawImage(inkRef.current, 0, 0);
    c.toBlob((b) => { const a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = (note.title || 'sketch').replace(/[^\w-]+/g, '-') + '.png'; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 4000); });
  };
  const onFile = (e) => { const f = e.target.files && e.target.files[0]; if (!f) return; const r = new FileReader(); r.onload = () => setBackground(r.result, false); r.readAsDataURL(f); e.target.value = ''; };
  const PRESETS = [['Heart & lungs', ART.heartLungs], ['Organs', ART.viscera], ['Vessels', ART.angiogram], ['Hand', ART.hand]];
  return html`<div className="sk">
    <div className="sk-bar">
      <div className="sk-tools">${[['pen', 'fa-pen', 'Pen'], ['hl', 'fa-highlighter', 'Highlighter'], ['eraser', 'fa-eraser', 'Eraser']].map(([k, ic, t]) => html`<button key=${k} className=${cx(tool === k && 'on')} onClick=${() => setTool(k)} title=${t} aria-label=${t}><${Icon} n=${ic} /></button>`)}</div>
      <div className="sk-cols">${SK_COLORS.map((c) => html`<button key=${c} className=${cx('dot', color === c && 'on')} style=${{ '--c': c }} onClick=${() => { setColor(c); if (tool === 'eraser') setTool('pen'); }} aria-label=${'Colour ' + c}></button>`)}</div>
      <input type="range" min="1" max="14" value=${size} onChange=${(e) => setSize(+e.target.value)} aria-label="Brush size" />
      <div className="sk-tools"><button onClick=${undoOne} title="Undo" aria-label="Undo"><${Icon} n="fa-rotate-left" /></button><button onClick=${clearAll} title="Clear" aria-label="Clear"><${Icon} n="fa-trash-can" /></button><button onClick=${exportPng} title="Download PNG" aria-label="Download PNG"><${Icon} n="fa-download" /></button></div>
    </div>
    <div className="sk-stage"><canvas ref=${bgRef} width=${SK_W} height=${SK_H}></canvas>
      <canvas ref=${inkRef} width=${SK_W} height=${SK_H} className=${cx('ink', tool)} onPointerDown=${down} onPointerMove=${move} onPointerUp=${up} onPointerCancel=${up}></canvas>
      ${!hasBg && html`<p className="sk-hint">Draw here, or pick a picture to annotate</p>`}</div>
    <div className="sk-src"><span>Draw on:</span>${PRESETS.map(([t, u]) => html`<button key=${t} onClick=${() => setBackground(u, true)}>${t}</button>`)}
      <button onClick=${() => fileRef.current.click()}><${Icon} n="fa-image" /> Your image</button><input ref=${fileRef} type="file" accept="image/*" hidden onChange=${onFile} /></div>
  </div>`;
}

/* ---------- notes ---------- */
function NotesTab() {
  const notes = useStore(NotesStore);
  const [id, setId] = useState(() => (NotesStore.get()[0] || {}).id || null);
  const note = notes.find((n) => n.id === id);
  const patch = (p) => {
    const next = NotesStore.get().map((n) => (n.id === id ? { ...n, ...p, updated: Date.now() } : n));
    try { localStorage.setItem('usmle-site:notes', JSON.stringify(next)); } catch { toast('This device is out of storage. Download the sketch, then clear some notes.'); return; }
    NotesStore.set(next);
  };
  const create = () => { const n = { id: uid(), title: '', text: '', ink: '', bg: '', updated: Date.now() }; NotesStore.set([n, ...NotesStore.get()]); setId(n.id); };
  const remove = (nid) => { if (!confirm('Delete this note?')) return; const rest = NotesStore.get().filter((n) => n.id !== nid); NotesStore.set(rest); if (id === nid) setId((rest[0] || {}).id || null); };
  return html`<div className="nt">
    <aside className="nt-list">
      <button className="nt-new" onClick=${create}><${Icon} n="fa-plus" /> New note</button>
      ${notes.length === 0 && html`<p className="st-empty">Your notes live here. Write, sketch, or draw on top of a picture.</p>`}
      ${notes.map((n) => html`<div key=${n.id} className=${cx('nt-item', n.id === id && 'on')} onClick=${() => setId(n.id)}>
        <b>${n.title || 'Untitled note'}</b><small>${new Date(n.updated).toLocaleDateString(undefined, { day: 'numeric', month: 'short' })}${n.ink ? ' · sketch' : ''}</small>
        <button onClick=${(e) => { e.stopPropagation(); remove(n.id); }} aria-label="Delete note"><${Icon} n="fa-xmark" /></button></div>`)}
    </aside>
    <section className="nt-edit">
      ${!note ? html`<p className="st-empty big">Start a note to begin.</p>` : html`<${Fragment} key=${note.id}>
        <input className="nt-title" value=${note.title} onChange=${(e) => patch({ title: e.target.value })} placeholder="Give it a title" />
        <textarea className="nt-text" value=${note.text} onChange=${(e) => patch({ text: e.target.value })} placeholder="Write anything: mnemonics, pathways, things to ask later…"></textarea>
        <${Sketch} note=${note} onInk=${patch} /><//>`}
    </section>
  </div>`;
}

/* ---------- flashcards (spaced repetition) ---------- */
function grade(c, q) {
  let { ivl = 0, ease = 2.4, reps = 0 } = c;
  if (q === 0) return { ...c, reps: 0, ivl: 0, ease: Math.max(1.3, ease - 0.2), due: Date.now() + 10 * 60000 };
  if (q === 1) { ivl = Math.max(1, Math.round(ivl * 1.2)); ease = Math.max(1.3, ease - 0.15); }
  else if (q === 2) ivl = reps === 0 ? 1 : reps === 1 ? 3 : Math.round(ivl * ease);
  else { ivl = reps === 0 ? 3 : reps === 1 ? 6 : Math.round(ivl * ease * 1.3); ease += 0.1; }
  return { ...c, reps: reps + 1, ivl, ease, due: Date.now() + ivl * DAY };
}
const hint = (c, q) => { if (q === 0) return '10m'; const g = grade(c, q); return g.ivl + 'd'; };
function CardsTab() {
  const cards = useStore(CardStore), ann = useStore(AnnStore);
  const [mode, setMode] = useState('review');
  const [flip, setFlip] = useState(false);
  const [f, setF] = useState(''), [b, setB] = useState('');
  const now = Date.now();
  const due = cards.filter((c) => c.due <= now).sort((a, c) => a.due - c.due);
  const cur = due[0];
  const rate = useCallback((q) => { if (!cur) return; CardStore.set(CardStore.get().map((c) => (c.id === cur.id ? grade(c, q) : c))); setFlip(false); }, [cur]);
  useEffect(() => {
    if (mode !== 'review') return undefined;
    const k = (e) => { if (/INPUT|TEXTAREA/.test(e.target.tagName)) return; if (e.key === ' ') { e.preventDefault(); setFlip((v) => !v); } else if (flip && '1234'.includes(e.key) && e.key) rate(+e.key - 1); };
    window.addEventListener('keydown', k); return () => window.removeEventListener('keydown', k);
  }, [mode, flip, rate]);
  const hlRows = Object.entries(ann).flatMap(([url, a]) => hlList(a).map((h) => ({ url, title: a.title || 'Saved page', h })))
    .filter((r) => !cards.some((c) => c.back === r.h.t));
  return html`<div className="cd">
    <div className="st-stats"><div><b>${due.length}</b><span>due now</span></div><div><b>${cards.length}</b><span>cards</span></div><div><b>${cards.filter((c) => c.ivl >= 21).length}</b><span>mature</span></div></div>
    <${Seg} value=${mode} onChange=${setMode} options=${[['review', 'Review'], ['add', 'Add'], ['hl', `From highlights${hlRows.length ? ' · ' + hlRows.length : ''}`], ['all', 'All cards']]} />
    ${mode === 'review' && (cur ? html`<div className="cd-review">
      <div className=${cx('cd-card', flip && 'flip')} onClick=${() => setFlip((v) => !v)} role="button" tabIndex="0" aria-label="Flip card">
        <div className="face front"><small>${cur.src || 'Question'}</small><p>${cur.front}</p><em>Tap or press space to flip</em></div>
        <div className="face back"><small>Answer</small><p>${cur.back}</p></div></div>
      <div className=${cx('cd-rate', flip && 'show')}>${['Again', 'Hard', 'Good', 'Easy'].map((t, q) => html`<button key=${t} onClick=${() => rate(q)}><b>${t}</b><small>${hint(cur, q)}</small></button>`)}</div>
    </div>` : html`<p className="st-empty big">${cards.length ? 'All caught up. Come back later, your next card is on its way.' : 'No cards yet. Add one, or turn a highlight into a card.'}</p>`)}
    ${mode === 'add' && html`<form className="cd-add" onSubmit=${(e) => { e.preventDefault(); if (f.trim() && b.trim() && addCard(f.trim(), b.trim(), 'My card')) { setF(''); setB(''); } }}>
      <textarea value=${f} onChange=${(e) => setF(e.target.value)} placeholder="Front: a question or a prompt"></textarea>
      <textarea value=${b} onChange=${(e) => setB(e.target.value)} placeholder="Back: the answer"></textarea>
      <button className="lh-btn" type="submit"><${Icon} n="fa-plus" /> Add card</button></form>`}
    ${mode === 'hl' && html`<div className="cd-list">${hlRows.length === 0 ? html`<p className="st-empty">Highlight text in the Library reader and it will show up here, ready to become a card.</p>` : hlRows.map((r) => html`<div key=${r.url + r.h.t} className="cd-row"><div><small>${r.title}</small><p>${r.h.t}</p>${r.h.n && html`<em>${r.h.n}</em>`}</div><button onClick=${() => addCard(r.h.n || 'Recall: ' + r.title, r.h.t, r.title)}><${Icon} n="fa-clone" /> Make card</button></div>`)}</div>`}
    ${mode === 'all' && html`<div className="cd-list">${cards.length === 0 ? html`<p className="st-empty">Nothing here yet.</p>` : cards.map((c) => html`<div key=${c.id} className="cd-row"><div><small>${c.src || 'Card'} · ${c.due <= now ? 'due now' : 'due ' + new Date(c.due).toLocaleDateString(undefined, { day: 'numeric', month: 'short' })}</small><p>${c.front}</p><em>${c.back}</em></div><button onClick=${() => CardStore.set(CardStore.get().filter((x) => x.id !== c.id))} aria-label="Delete card"><${Icon} n="fa-trash-can" /></button></div>`)}</div>`}
  </div>`;
}

/* ---------- mistake notebook ---------- */
function MistakesTab({ go, openCase }) {
  const attempts = useAttempts().filter((a) => !a.sample);
  const mastered = useStore(MasteredStore);
  const rows = CASES.map((c, i) => {
    let miss = 0, seen = 0, lastMiss = 0, lastOk = 0;
    attempts.forEach((a) => { if (i >= a.total) return; seen += 1; if ((a.correct || []).includes(i)) lastOk = a.timestamp; else { miss += 1; lastMiss = a.timestamp; } });
    return { c, miss, seen, still: lastMiss > lastOk, lastMiss };
  }).filter((r) => r.miss > 0 && !mastered.includes(r.c.id)).sort((a, b) => (b.still - a.still) || (b.miss - a.miss));
  if (!attempts.length) return html`<p className="st-empty big">Finish a quiz and the questions you miss will collect here, with the reasoning ready to review.</p>`;
  if (!rows.length) return html`<p className="st-empty big">No open mistakes. Nicely done.</p>`;
  return html`<div className="mk">${rows.map(({ c, miss, seen, still }) => html`<article key=${c.id} className="mk-card">
    <div className="mk-top"><span className=${cx('mk-tag', still && 'hot')}>${still ? 'Still tricky' : 'Got it since'}</span><span className="mk-n">Missed ${miss} of ${seen}</span></div>
    <h4>${c.title}</h4><p>${c.objective || c.tag}</p>
    <div className="mk-act"><button onClick=${() => { openCase(c.index); go('library'); }}><${Icon} n="fa-book-open" /> Read the case</button>
      <button onClick=${() => addCard(`${c.title}: what is the key idea?`, c.objective || c.tag, c.code)}><${Icon} n="fa-clone" /> Make a card</button>
      <button onClick=${() => MasteredStore.set([...MasteredStore.get(), c.id])}><${Icon} n="fa-check" /> Mastered</button></div></article>`)}</div>`;
}

/* ---------- planner ---------- */
function PlannerTab() {
  const plan = useStore(PlanStore), attempts = useAttempts().filter((a) => !a.sample), log = useStore(FocusLog), cards = useStore(CardStore);
  const [t, setT] = useState('');
  const set = (p) => PlanStore.set({ ...PlanStore.get(), ...p });
  const days = plan.exam ? Math.ceil((new Date(plan.exam + 'T00:00:00') - new Date(new Date().toDateString())) / DAY) : null;
  const qToday = attempts.filter((a) => dayKey(a.timestamp) === new Date().toDateString()).reduce((s, a) => s + a.total, 0);
  const week = Array.from({ length: 7 }, (_, k) => { const d = new Date(); d.setDate(d.getDate() - (6 - k)); const key = d.toDateString(); return { d, q: attempts.filter((a) => dayKey(a.timestamp) === key).reduce((s, a) => s + a.total, 0), m: Math.round(log.filter((f) => dayKey(f.at) === key).reduce((s, f) => s + f.sec, 0) / 60) }; });
  const maxM = Math.max(30, ...week.map((w) => w.m));
  return html`<div className="pl">
    <div className="pl-count"><small>Your exam</small>${days === null ? html`<p>Pick your Step 1 date to start the countdown.</p>` : days < 0 ? html`<b>Done</b><p>Your exam date has passed. Well done for getting there.</p>` : html`<b>${days}</b><p>${days === 1 ? 'day' : 'days'} to go · about ${Math.max(1, Math.round(days / 7))} week${Math.round(days / 7) === 1 ? '' : 's'}</p>`}
      <${DatePicker} value=${plan.exam} onChange=${(v) => set({ exam: v })} placeholder="Pick your exam date" label="Exam date" /></div>
    <div className="pl-goal"><small>Today</small>
      <div className="pl-ring" style=${{ '--p': Math.min(1, qToday / Math.max(1, plan.goal)) }}><b>${qToday}</b><span>of ${plan.goal} questions</span></div>
      <label>Daily goal <input type="number" min="1" max="400" value=${plan.goal} onChange=${(e) => set({ goal: Math.max(1, +e.target.value || 1) })} /></label>
      <p>${focusToday(log)} min focused · ${cards.filter((c) => c.due <= Date.now()).length} cards due</p></div>
    <div className="pl-week"><small>This week</small><div>${week.map((w) => html`<span key=${w.d} title=${w.m + ' min, ' + w.q + ' questions'}><i style=${{ height: Math.max(4, (w.m / maxM) * 100) + '%' }}></i><em>${w.d.toLocaleDateString(undefined, { weekday: 'narrow' })}</em></span>`)}</div></div>
    <div className="pl-tasks"><small>Checklist</small>
      <form onSubmit=${(e) => { e.preventDefault(); if (t.trim()) { set({ tasks: [...plan.tasks, { id: uid(), t: t.trim(), done: false }] }); setT(''); } }}><input value=${t} onChange=${(e) => setT(e.target.value)} placeholder="Add something to do…" /><button className="lh-btn" type="submit"><${Icon} n="fa-plus" /></button></form>
      ${plan.tasks.length === 0 && html`<p className="st-empty">Break the big goal into small wins.</p>`}
      ${plan.tasks.map((x) => html`<label key=${x.id} className=${cx('pl-task', x.done && 'done')}><input type="checkbox" checked=${x.done} onChange=${() => set({ tasks: plan.tasks.map((y) => (y.id === x.id ? { ...y, done: !y.done } : y)) })} /><span>${x.t}</span><button type="button" onClick=${() => set({ tasks: plan.tasks.filter((y) => y.id !== x.id) })} aria-label="Remove"><${Icon} n="fa-xmark" /></button></label>`)}</div>
  </div>`;
}

/* ---------- pomodoro ---------- */
const POMO = { focus: 25, short: 5, long: 15 };
function beep() { if (!PrefsStore.get().sound) return; try { const a = new (window.AudioContext || window.webkitAudioContext)(); [0, 0.22, 0.44].forEach((d) => { const o = a.createOscillator(), g = a.createGain(); o.frequency.value = 660 + d * 400; g.gain.value = 0.06; o.connect(g); g.connect(a.destination); o.start(a.currentTime + d); o.stop(a.currentTime + d + 0.15); }); } catch { /* no audio */ } }
function FocusTab() {
  const log = useStore(FocusLog);
  const [mode, setMode] = useState('focus');
  const [lens, setLens] = usePref('pomo', POMO);
  const [left, setLeft] = useState(lens.focus * 60);
  const [on, setOn] = useState(false);
  const [round, setRound] = useState(0);
  const end = useRef(0);
  const total = lens[mode] * 60;
  useEffect(() => { if (!on) setLeft(lens[mode] * 60); }, [mode, lens.focus, lens.short, lens.long]);
  useEffect(() => {
    if (!on) return undefined;
    end.current = Date.now() + left * 1000;
    const id = setInterval(() => {
      const r = Math.max(0, Math.round((end.current - Date.now()) / 1000)); setLeft(r);
      if (r <= 0) {
        clearInterval(id); beep(); setOn(false);
        if (mode === 'focus') { logFocus(lens.focus * 60); const n = round + 1; setRound(n); toast('Focus block done. Take a breather.'); setMode(n % 4 === 0 ? 'long' : 'short'); } else { toast('Break over. Ready when you are.'); setMode('focus'); }
      }
    }, 250);
    return () => clearInterval(id);
  }, [on, mode]);
  useEffect(() => { document.title = on ? `${pad2(Math.floor(left / 60))}:${pad2(left % 60)} · Focus` : 'USMLE Quiz'; return () => { document.title = 'USMLE Quiz'; }; }, [on, left]);
  const R = 120, C = 2 * Math.PI * R;
  const stop = () => { if (on && mode === 'focus') logFocus(total - left); setOn(false); setLeft(total); };
  return html`<div className="fc">
    <${Seg} value=${mode} onChange=${(m) => { if (!on) setMode(m); }} options=${[['focus', 'Focus'], ['short', 'Short break'], ['long', 'Long break']]} />
    <div className="fc-ring"><svg viewBox="0 0 280 280"><circle cx="140" cy="140" r=${R} className="bg" /><circle cx="140" cy="140" r=${R} className="fg" style=${{ strokeDasharray: C, strokeDashoffset: C * (1 - (total - left) / total) }} /></svg>
      <div className="fc-time"><b>${pad2(Math.floor(left / 60))}:${pad2(left % 60)}</b><span>${mode === 'focus' ? 'Deep focus' : 'Rest your eyes'}</span></div></div>
    <div className="fc-ctl"><button className="lh-btn" onClick=${() => setOn((v) => !v)}><${Icon} n=${on ? 'fa-pause' : 'fa-play'} /> ${on ? 'Pause' : left < total ? 'Resume' : 'Start'}</button><button className="fc-reset" onClick=${stop} aria-label="Reset"><${Icon} n="fa-rotate-left" /></button></div>
    <div className="fc-set">${Object.entries({ focus: 'Focus', short: 'Short', long: 'Long' }).map(([k, l]) => html`<label key=${k}>${l}<input type="number" min="1" max="90" value=${lens[k]} disabled=${on} onChange=${(e) => setLens({ ...lens, [k]: Math.max(1, Math.min(90, +e.target.value || 1)) })} /><small>min</small></label>`)}</div>
    <p className="fc-sum">Today: <b>${focusToday(log)} min</b> focused · Blocks this session: <b>${round}</b> · All time: <b>${Math.round(log.reduce((s, f) => s + f.sec, 0) / 3600 * 10) / 10} h</b></p>
  </div>`;
}

/* A calm, rounded calendar that replaces the browser's date box. Values are "YYYY-MM-DD" strings. */
const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const isoOf = (y, m, d) => y + '-' + pad2(m + 1) + '-' + pad2(d);
function DatePicker({ value, onChange, placeholder = 'Pick a date', label }) {
  const today = new Date();
  const parse = (v) => { const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(v || ''); return m ? { y: +m[1], m: +m[2] - 1, d: +m[3] } : null; };
  const sel = parse(value);
  const [open, setOpen] = useState(false);
  const [view, setView] = useState({ y: (sel || { y: today.getFullYear() }).y, m: (sel || { m: today.getMonth() }).m, mode: 'days' });
  const [pos, setPos] = useState(null);
  const btn = useRef(null);
  const place = useCallback(() => {
    if (!btn.current) return; const r = btn.current.getBoundingClientRect(), W = 320, H = 392;
    const left = Math.min(Math.max(8, r.left), window.innerWidth - W - 8);
    const below = r.bottom + 8 + H < window.innerHeight || r.top < H + 16;
    setPos({ left, top: below ? r.bottom + 8 : r.top - H - 8, w: Math.max(W, 0) });
  }, []);
  useEffect(() => {
    if (!open) return undefined;
    place();
    const k = (e) => { if (e.key === 'Escape') { e.stopPropagation(); setOpen(false); } };
    const d = (e) => { if (!e.target.closest('.dp-pop') && !(btn.current && btn.current.contains(e.target))) setOpen(false); };
    window.addEventListener('keydown', k, true); document.addEventListener('mousedown', d); window.addEventListener('resize', place); window.addEventListener('scroll', place, true);
    return () => { window.removeEventListener('keydown', k, true); document.removeEventListener('mousedown', d); window.removeEventListener('resize', place); window.removeEventListener('scroll', place, true); };
  }, [open, place]);
  const toggle = () => { if (!open) { const s2 = parse(value); setView({ y: (s2 || { y: today.getFullYear() }).y, m: (s2 || { m: today.getMonth() }).m, mode: 'days' }); } setOpen((o) => !o); };
  const pick = (y, m, d) => { onChange(isoOf(y, m, d)); setOpen(false); };
  const shift = (n) => { if (view.mode === 'days') { const t = new Date(view.y, view.m + n, 1); setView({ ...view, y: t.getFullYear(), m: t.getMonth() }); } else if (view.mode === 'months') setView({ ...view, y: view.y + n }); else setView({ ...view, y: view.y + n * 12 }); };
  const first = new Date(view.y, view.m, 1).getDay(), dim = new Date(view.y, view.m + 1, 0).getDate();
  const cells = [...Array(first).fill(null), ...Array.from({ length: dim }, (_, i) => i + 1)];
  const y0 = view.y - (view.y % 12);
  const pretty = sel ? new Date(sel.y, sel.m, sel.d).toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric' }) : '';
  const isT = (d) => d && today.getFullYear() === view.y && today.getMonth() === view.m && today.getDate() === d;
  const isS = (d) => d && sel && sel.y === view.y && sel.m === view.m && sel.d === d;
  return html`<${Fragment}>
    <button type="button" ref=${btn} className=${cx('dp-btn', open && 'open')} onClick=${toggle} aria-haspopup="dialog" aria-expanded=${open} aria-label=${label || placeholder}>
      <${Icon} n="fa-calendar-days" /><span className=${cx(!sel && 'ph')}>${pretty || placeholder}</span></button>
    ${open && pos && ReactDOM.createPortal(html`<div className="dp-pop" style=${{ left: pos.left, top: pos.top }} role="dialog" aria-label="Choose a date">
      <div className="dp-head"><button type="button" onClick=${() => shift(-1)} aria-label="Previous"><${Icon} n="fa-chevron-left" /></button>
        <button type="button" className="dp-title" onClick=${() => setView({ ...view, mode: view.mode === 'days' ? 'months' : view.mode === 'months' ? 'years' : 'days' })}>${view.mode === 'days' ? MONTHS[view.m] + ' ' + view.y : view.mode === 'months' ? view.y : y0 + ' – ' + (y0 + 11)}</button>
        <button type="button" onClick=${() => shift(1)} aria-label="Next"><${Icon} n="fa-chevron-right" /></button></div>
      ${view.mode === 'days' && html`<div className="dp-grid"><${Fragment}>${['S', 'M', 'T', 'W', 'T', 'F', 'S'].map((w, i) => html`<em key=${'w' + i}>${w}</em>`)}${cells.map((d, i) => d ? html`<button type="button" key=${i} className=${cx(isS(d) && 'sel', isT(d) && 'today')} onClick=${() => pick(view.y, view.m, d)}>${d}</button>` : html`<span key=${i}></span>`)}<//></div>`}
      ${view.mode === 'months' && html`<div className="dp-grid big">${MONTHS.map((m, i) => html`<button type="button" key=${m} className=${cx(sel && sel.y === view.y && sel.m === i && 'sel')} onClick=${() => setView({ ...view, m: i, mode: 'days' })}>${m.slice(0, 3)}</button>`)}</div>`}
      ${view.mode === 'years' && html`<div className="dp-grid big">${Array.from({ length: 12 }, (_, i) => y0 + i).map((y) => html`<button type="button" key=${y} className=${cx(sel && sel.y === y && 'sel')} onClick=${() => setView({ ...view, y, mode: 'months' })}>${y}</button>`)}</div>`}
      <div className="dp-foot"><button type="button" onClick=${() => { onChange(''); setOpen(false); }}>Clear</button><button type="button" onClick=${() => pick(today.getFullYear(), today.getMonth(), today.getDate())}>Today</button></div>
    </div>`, document.body)}
  <//>`;
}

/* ==========================================================================
   ACCOUNT: your profile home, progress, goals, settings and your data
   ========================================================================== */
function SelectBox({ value, options, onChange, placeholder = 'Choose', label }) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState(null);
  const [hi, setHi] = useState(-1);
  const btn = useRef(null);
  const place = useCallback(() => {
    if (!btn.current) return; const r = btn.current.getBoundingClientRect(), H = Math.min(options.length * 46 + 16, 420);
    const below = r.bottom + 8 + H < window.innerHeight || r.top < H + 16;
    setPos({ left: r.left, width: r.width, top: below ? r.bottom + 8 : r.top - H - 8 });
  }, [options.length]);
  useEffect(() => {
    if (!open) return undefined;
    place(); setHi(Math.max(0, options.findIndex((o) => o[0] === value)));
    const k = (e) => {
      if (e.key === 'Escape') { e.stopPropagation(); setOpen(false); btn.current && btn.current.focus(); }
      else if (e.key === 'ArrowDown') { e.preventDefault(); setHi((h) => Math.min(options.length - 1, h + 1)); }
      else if (e.key === 'ArrowUp') { e.preventDefault(); setHi((h) => Math.max(0, h - 1)); }
      else if (e.key === 'Enter') { e.preventDefault(); setHi((h) => { if (options[h]) { onChange(options[h][0]); setOpen(false); } return h; }); }
    };
    const d = (e) => { if (!e.target.closest('.sb-pop') && !(btn.current && btn.current.contains(e.target))) setOpen(false); };
    window.addEventListener('keydown', k, true); document.addEventListener('mousedown', d); window.addEventListener('resize', place); window.addEventListener('scroll', place, true);
    return () => { window.removeEventListener('keydown', k, true); document.removeEventListener('mousedown', d); window.removeEventListener('resize', place); window.removeEventListener('scroll', place, true); };
  }, [open, place]);
  const cur = options.find((o) => o[0] === value);
  return html`<${Fragment}>
    <button type="button" ref=${btn} className=${cx('sb-btn', open && 'open')} onClick=${() => setOpen((o) => !o)} aria-haspopup="listbox" aria-expanded=${open} aria-label=${label || placeholder}>
      <span className=${cx(!cur && 'ph')}>${cur ? cur[1] : placeholder}</span><${Icon} n="fa-chevron-down" cls="fas sb-chev" /></button>
    ${open && pos && ReactDOM.createPortal(html`<ul className="sb-pop" role="listbox" style=${{ left: pos.left, top: pos.top, width: pos.width }}>
      ${options.map(([k, l], i) => html`<li key=${k} role="option" aria-selected=${k === value}><button type="button" className=${cx(k === value && 'on', i === hi && 'hi')} onMouseEnter=${() => setHi(i)} onClick=${() => { onChange(k); setOpen(false); }}><span>${l}</span>${k === value && html`<${Icon} n="fa-check" />`}</button></li>`)}</ul>`, document.body)}
  <//>`;
}

const fmtKB = (n) => (n < 1024 ? n + ' B' : n < 1048576 ? (n / 1024).toFixed(1) + ' KB' : (n / 1048576).toFixed(2) + ' MB');
const sizeOf = (v) => { try { return new Blob([JSON.stringify(v)]).size; } catch { return 0; } };
const SYNC_LABELS = { attempts: 'Quiz scores', saved: 'Saved pages', annotations: 'Highlights', notes: 'Notes and sketches', cards: 'Flashcards', focus: 'Focus log', mastered: 'Mastered', goals: 'Goals', prefs: 'Settings', plan: 'Planner', name: 'Name' };
const DAYS7 = () => Array.from({ length: 7 }, (_, k) => { const d = new Date(); d.setDate(d.getDate() - (6 - k)); return d; });

function exportMine() {
  const out = { exported: new Date().toISOString(), note: 'Your Step 1 study data. It is yours to keep.', profile: (CloudStore.get().profile || null) };
  Object.entries(SYNC).forEach(([k, def]) => { out[k] = def.st.get(); });
  const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob([JSON.stringify(out, null, 2)], { type: 'application/json' }));
  a.download = 'step1-my-data-' + new Date().toISOString().slice(0, 10) + '.json'; a.click(); setTimeout(() => URL.revokeObjectURL(a.href), 4000);
}

function ProfileCard({ go }) {
  const [qr, setQr] = useState(false);
  const c = useCloud(), name = useUserName(), attempts = useAttempts().filter((a) => !a.sample), cards = useStore(CardStore), log = useStore(FocusLog), plan = useStore(PlanStore), goals = useStore(GoalsStore), notes = useStore(NotesStore);
  const p = c.profile || {};
  const st = summarize(attempts);
  const days = plan.exam ? Math.ceil((new Date(plan.exam + 'T00:00:00') - new Date(new Date().toDateString())) / DAY) : null;
  const due = cards.filter((x) => x.due <= Date.now()).length;
  const last = notes[0];
  const avatar = p.avatar_url ? html`<img src=${p.avatar_url} alt="" />` : html`<b>${(name || '?').charAt(0).toUpperCase()}</b>`;
  return html`<div className="bn">
    <section className="bn-hero">
      <div className="bn-top"><div className="bn-stat"><b>${st.n}</b><span>quizzes</span></div><div className="bn-av"><span>${avatar}</span></div><div className="bn-stat"><b>${st.n ? st.best + '%' : '–'}</b><span>best score</span></div></div>
      <h2>${name}${c.user && html`<i className="bn-vf" title="Signed in"><svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round"><path d="M5 12.5l4.5 4.5L19 7.5"/></svg></i>`}</h2>
      <p className="bn-sub">${c.user ? (p.username ? '@' + p.username : 'No username yet') + (p.study_year ? ' · ' + p.study_year : '') : 'Studying on this device'}</p>
      <p className="bn-bio">${p.bio || (c.user ? 'Add a line about you and your goals.' : 'Sign in to add a photo and a username, and to keep everything on every device.')}</p>
      <div className="bn-btns">${c.user ? html`<button onClick=${() => patchCloud({ profileOpen: true })}>Edit profile</button>` : html`<button onClick=${() => patchCloud({ modal: true })}>Sign in</button>`}<button onClick=${() => go('community')}>Community</button>${c.user && html`<button onClick=${() => setQr(true)} aria-label="Share my profile code"><${Icon} n="fa-qrcode" /></button>`}</div>
    </section>
    ${qr && ReactDOM.createPortal(html`<div className="au-back" onMouseDown=${(e) => { if (e.target === e.currentTarget) setQr(false); }}><div className="au-card qr-modal"><button type="button" className="au-x" onClick=${() => setQr(false)} aria-label="Close"><${Icon} n="fa-xmark" /></button><h3>Share your profile</h3><${QrCard} username=${p.username} /></div></div>`, document.body)}
    <article className="bn-tile why"><small>Your why</small><p>${goals.why || 'Write the reason you are doing this. It will be here on the hard days.'}</p></article>
    <article className="bn-tile streak"><small>Streak</small><b>${st.streak}</b><span>day${st.streak === 1 ? '' : 's'} in a row</span></article>
    <article className="bn-tile exam"><small>Step 1</small><b>${days !== null && days >= 0 ? days : '–'}</b><span>${days === null ? 'set your date' : days < 0 ? 'done' : 'days to go'}</span></article>
    <article className="bn-tile cards"><small>Flashcards</small><b>${due}</b><span>due now</span><button onClick=${() => { PrefsStore.get(); go('study'); }}>Review</button></article>
    <article className="bn-tile focus"><small>Focus today</small><b>${focusToday(log)}</b><span>minutes</span></article>
    <article className="bn-tile note"><small>Latest note</small><p>${last ? (last.title || 'Untitled note') : 'Nothing yet. Your notes and sketches will show up here.'}</p><button onClick=${() => go('study')}>${last ? 'Open' : 'Start one'}</button></article>
  </div>`;
}

function GoalsTab() {
  const g = useStore(GoalsStore), plan = useStore(PlanStore), attempts = useAttempts().filter((a) => !a.sample), log = useStore(FocusLog);
  const [m, setM] = useState('');
  const set = (p) => GoalsStore.set({ ...GoalsStore.get(), ...p });
  const avg = attempts.length ? Math.round(attempts.slice(-10).reduce((s, a) => s + pctOf(a), 0) / Math.min(10, attempts.length)) : 0;
  const weekMin = Math.round(log.filter((f) => Date.now() - f.at < 7 * DAY).reduce((s, f) => s + f.sec, 0) / 60);
  const qToday = attempts.filter((a) => dayKey(a.timestamp) === new Date().toDateString()).reduce((s, a) => s + a.total, 0);
  const Bar = ({ v, max, label, now }) => html`<div className="gl-bar"><div><b>${label}</b><span>${now}</span></div><i><u style=${{ width: Math.min(100, (v / Math.max(1, max)) * 100) + '%' }}></u></i></div>`;
  return html`<div className="gl">
    <div className="gl-card"><small>Your why</small><textarea value=${g.why} maxLength="240" onChange=${(e) => set({ why: e.target.value })} placeholder="Why are you doing this? On the hard days, this is what you will want to read."></textarea></div>
    <div className="gl-card"><small>Targets</small>
      <label>Practice accuracy<span><input type="number" min="30" max="100" value=${g.accuracy} onChange=${(e) => set({ accuracy: Math.max(30, Math.min(100, +e.target.value || 30)) })} /> %</span></label>
      <label>Questions a day<span><input type="number" min="1" max="400" value=${plan.goal} onChange=${(e) => PlanStore.set({ ...PlanStore.get(), goal: Math.max(1, +e.target.value || 1) })} /></span></label>
      <label>Focus hours a week<span><input type="number" min="1" max="100" value=${g.weeklyHours} onChange=${(e) => set({ weeklyHours: Math.max(1, +e.target.value || 1) })} /></span></label></div>
    <div className="gl-card wide"><small>How you are doing</small>
      <${Bar} v=${avg} max=${g.accuracy} label="Accuracy, last 10 quizzes" now=${avg + '% of ' + g.accuracy + '%'} />
      <${Bar} v=${qToday} max=${plan.goal} label="Questions today" now=${qToday + ' of ' + plan.goal} />
      <${Bar} v=${weekMin} max=${g.weeklyHours * 60} label="Focus this week" now=${Math.round(weekMin / 6) / 10 + ' of ' + g.weeklyHours + ' h'} /></div>
    <div className="gl-card wide"><small>Milestones</small>
      <form onSubmit=${(e) => { e.preventDefault(); if (m.trim()) { set({ milestones: [...g.milestones, { id: uid(), t: m.trim(), done: false, due: '' }] }); setM(''); } }}><input value=${m} onChange=${(e) => setM(e.target.value)} placeholder="Finish cardiology, 1000 questions, first 80%…" /><button className="lh-btn" type="submit"><${Icon} n="fa-plus" /></button></form>
      ${g.milestones.length === 0 && html`<p className="st-empty">Small, dated wins keep the big goal close.</p>`}
      ${g.milestones.map((x) => html`<div key=${x.id} className=${cx('gl-ms', x.done && 'done')}><label><input type="checkbox" checked=${x.done} onChange=${() => set({ milestones: g.milestones.map((y) => (y.id === x.id ? { ...y, done: !y.done } : y)) })} /><span>${x.t}</span></label>
        <${DatePicker} value=${x.due} onChange=${(v) => set({ milestones: g.milestones.map((y) => (y.id === x.id ? { ...y, due: v } : y)) })} placeholder="Add date" label="Milestone date" />
        <button onClick=${() => set({ milestones: g.milestones.filter((y) => y.id !== x.id) })} aria-label="Remove"><${Icon} n="fa-xmark" /></button></div>`)}</div>
  </div>`;
}

function PasskeyBlock() {
  const [list, setList] = useState(null), [msg, setMsg] = useState(''), [busy, setBusy] = useState(false);
  const load = useCallback(async () => { try { const { data, error } = await sb.auth.passkey.list(); setList(error ? [] : data || []); if (error && !/passkey_disabled/i.test(error.message || '')) setMsg(error.message); } catch { setList([]); } }, []);
  useEffect(() => { load(); }, []);
  const add = async () => {
    setMsg('');
    if (!/^https?:$/.test(location.protocol)) { setMsg('Passkeys need the site to be hosted at its web address. They cannot be created from a file on your computer.'); return; }
    setBusy(true);
    try { const { error } = await sb.auth.registerPasskey(); if (error) throw error; toast('Passkey added'); load(); }
    catch (x) { const m = String(x.message || x); setMsg(/passkey_disabled/i.test(m) ? 'Passkeys are not switched on for this site yet.' : /NotAllowed|cancel|abort/i.test(m) ? 'The passkey prompt was closed.' : /exists/i.test(m) ? 'This device already has a passkey for your account.' : m); }
    finally { setBusy(false); }
  };
  const rename = async (p) => { const n = prompt('Name this passkey', p.friendly_name || ''); if (n && n.trim()) { await sb.auth.passkey.update({ passkeyId: p.id, friendlyName: n.trim().slice(0, 120) }); load(); } };
  const del = async (p) => { if (confirm('Remove this passkey? You can still sign in with your password.')) { await sb.auth.passkey.delete({ passkeyId: p.id }); load(); } };
  return html`<div className="ac-card col pk"><div className="pk-head"><div><b>Passkeys</b><small>Sign in with your fingerprint, face or device PIN instead of a password. Phishing-proof, and nothing to remember.</small></div><button className="lh-btn" onClick=${add} disabled=${busy}><${Icon} n="fa-fingerprint" /> ${busy ? 'Waiting…' : 'Add a passkey'}</button></div>
    ${msg && html`<p className="au-note">${msg}</p>`}
    ${list && list.length > 0 && html`<ul className="pk-list">${list.map((p) => html`<li key=${p.id}><${Icon} n="fa-fingerprint" /><div><b>${p.friendly_name || 'Passkey'}</b><small>Added ${new Date(p.created_at).toLocaleDateString(undefined, { day: 'numeric', month: 'short', year: 'numeric' })}${p.last_used_at ? ' · used ' + new Date(p.last_used_at).toLocaleDateString(undefined, { day: 'numeric', month: 'short' }) : ''}</small></div><button onClick=${() => rename(p)} aria-label="Rename"><${Icon} n="fa-pen" /></button><button onClick=${() => del(p)} aria-label="Remove"><${Icon} n="fa-trash-can" /></button></li>`)}</ul>`}</div>`;
}

function SettingsTab() {
  const pr = useStore(PrefsStore), cl = useCloud();
  const set = (p) => PrefsStore.set({ ...PrefsStore.get(), ...p });
  const Row = ({ k, title, sub }) => html`<label className="ac-row"><div><b>${title}</b><small>${sub}</small></div><button type="button" role="switch" aria-checked=${!!pr[k]} className=${cx('ac-sw', pr[k] && 'on')} onClick=${() => set({ [k]: !pr[k] })}><i></i></button></label>`;
  useEffect(() => { document.body.classList.toggle('rm', !!pr.reduceMotion); }, [pr.reduceMotion]);
  return html`<div className="ac-list">
    <${Row} k="sound" title="Timer sounds" sub="A soft chime when a focus block or break ends." />
    <${Row} k="collapseNav" title="Fold the menu as you scroll" sub="The navigation shrinks into the stack button while you read." />
    <${Row} k="reduceMotion" title="Calmer motion" sub="Turns off most animation across the site." />
    ${cl.user && html`<label className="ac-row"><div><b>Let students match with me</b><small>Shares your username, photo, bio and a summary of your study time, scores, streak and goals with students you could match with. Off by default.</small></div><button type="button" role="switch" aria-checked=${!!(cl.profile && cl.profile.discoverable)} className=${cx('ac-sw', cl.profile && cl.profile.discoverable && 'on')} onClick=${() => setDiscoverable(!(cl.profile && cl.profile.discoverable))}><i></i></button></label>`}
    ${cl.user && html`<label className="ac-row"><div><b>Show me on the community globe</b><small>Counts you in your country and state, nothing finer. Others only see totals, and places with fewer than three students stay hidden.</small></div><button type="button" role="switch" aria-checked=${pr.globe !== false} className=${cx('ac-sw', pr.globe !== false && 'on')} onClick=${async () => { const on = pr.globe === false; set({ globe: on }); try { if (on) await placeMe(cl.user, false); else { await leaveGlobe(cl.user); toast('You are off the globe.'); } } catch (x) { toast('Could not update the globe just now.'); } }}><i></i></button></label>`}
    <div className="ac-row"><div><b>Open the site on</b><small>The page you land on when you arrive.</small></div><div className="ac-sel"><${SelectBox} value=${pr.landing} onChange=${(v) => set({ landing: v })} label="Landing page" options=${[['home', 'Home'], ['quiz', 'Quiz'], ['dashboard', 'Dashboard'], ['study', 'Study'], ['library', 'Library']]} /></div></div>
    ${cl.user && html`<${PasskeyBlock} />`}
  </div>`;
}

function DataTab() {
  const c = useCloud(), all = {};
  Object.entries(SYNC).forEach(([k, def]) => { all[k] = sizeOf(def.st.get()); });
  const total = Object.values(all).reduce((s, n) => s + n, 0);
  const top = Object.entries(all).filter(([, n]) => n > 2).sort((a, b) => b[1] - a[1]);
  const [del, setDel] = useState(false), [typed, setTyped] = useState(''), [busy, setBusy] = useState(false), [err, setErr] = useState('');
  const doDelete = async () => {
    setBusy(true); setErr('');
    try {
      const uid = c.user.id;
      await sb.storage.from('avatars').remove([uid + '/avatar.jpg']).catch(() => {});
      const { error } = await sb.rpc('delete_my_account');
      if (error) throw error;
      await sb.auth.signOut().catch(() => {});
      Object.keys(localStorage).filter((k) => /^usmle-(site|quiz):/.test(k)).forEach((k) => localStorage.removeItem(k));
      location.hash = '#/home'; location.reload();
    } catch (x) { setErr(/delete_my_account|function/i.test(String(x.message || x)) ? 'Account deletion has not been switched on for this project yet.' : String(x.message || x)); setBusy(false); }
  };
  return html`<div className="ac-list">
    <div className="ac-card"><div><b>Export my data</b><small>Everything in one file you can keep: scores, notes, sketches, cards, highlights, goals and settings.</small></div><button className="lh-btn" onClick=${exportMine}><${Icon} n="fa-download" /> Download</button></div>
    <div className="ac-card col"><div><b>How much space your data uses</b><small>${fmtKB(total)} in total. Notes with sketches are the heaviest. Saved pages keep only a short preview, and the highlight keeps just the marked sentence, never the whole article.</small></div>
      <div className="ac-use">${top.map(([k, n]) => html`<div key=${k}><span>${SYNC_LABELS[k] || k}</span><i><u style=${{ width: Math.max(3, (n / Math.max(1, top[0][1])) * 100) + '%' }}></u></i><em>${fmtKB(n)}</em></div>`)}</div></div>
    <div className="ac-card"><div><b>Clear this device</b><small>Removes the copy of your data stored in this browser. If you are signed in, your account keeps its copy.</small></div><button className="lh-btn ghost" onClick=${() => { if (confirm('Remove all study data stored in this browser?')) { Object.keys(localStorage).filter((k) => /^usmle-(site|quiz):/.test(k)).forEach((k) => localStorage.removeItem(k)); location.reload(); } }}><${Icon} n="fa-eraser" /> Clear</button></div>
    ${c.user && html`<div className="ac-card danger"><div><b>Delete my account</b><small>Permanently removes your account, profile, photo and everything synced. This cannot be undone. Download your data first.</small></div>
      ${!del ? html`<button className="lh-btn danger" onClick=${() => setDel(true)}><${Icon} n="fa-trash-can" /> Delete account</button>` : html`<div className="ac-confirm"><input value=${typed} onChange=${(e) => setTyped(e.target.value)} placeholder="Type DELETE to confirm" /><button className="lh-btn danger" disabled=${typed !== 'DELETE' || busy} onClick=${doDelete}>${busy ? 'Deleting…' : 'Delete forever'}</button><button className="lh-btn ghost" onClick=${() => { setDel(false); setTyped(''); }}>Cancel</button>${err && html`<p className="au-err">${err}</p>`}</div>`}</div>`}
  </div>`;
}

function AccountPage({ go, openFind }) {
  const [tab, setTab] = usePref('acctTab', 'profile');
  return html`<section className="sec lib study acct" data-tone="paper" data-nav="light" style=${{ minHeight: '100dvh' }}>
    <div className="wrap"><p className="eyebrow">Your account</p><h1 className="st-h">Your <span className="hl">space.</span></h1>
      <div className="st-tabs"><${Seg} value=${tab} onChange=${setTab} options=${[['profile', 'Profile'], ['progress', 'Progress'], ['goals', 'Goals'], ['settings', 'Settings'], ['data', 'Data and privacy']]} /></div>
      ${tab === 'profile' && html`<${ProfileCard} go=${go} />`}
      ${tab === 'progress' && html`<div className="ac-embed"><${Dashboard} go=${go} openFind=${openFind} /></div>`}
      ${tab === 'goals' && html`<${GoalsTab} />`}
      ${tab === 'settings' && html`<${SettingsTab} />`}
      ${tab === 'data' && html`<${DataTab} />`}
    </div></section>`;
}

/* ==========================================================================
   COMMUNITY: an opt-in globe. Only a country is ever stored, never a place.
   ========================================================================== */
const loadScript = (src) => new Promise((ok, no) => {
  const ex = document.querySelector('script[data-src="' + src + '"]');
  if (ex && ex.dataset.ready) return ok();
  const s = ex || document.createElement('script');
  s.addEventListener('load', () => { s.dataset.ready = '1'; ok(); }); s.addEventListener('error', () => no(new Error('Could not load a map library')));
  if (!ex) { s.src = src; s.dataset.src = src; document.head.appendChild(s); }
});
let worldPromise = null;
const loadWorld = () => worldPromise || (worldPromise = (async () => {
  await loadScript('https://cdn.jsdelivr.net/npm/d3@7.9.0/dist/d3.min.js');
  await loadScript('https://cdn.jsdelivr.net/npm/topojson-client@3.1.0/dist/topojson-client.min.js');
  const r = await fetch('https://cdn.jsdelivr.net/npm/world-atlas@2.0.2/countries-110m.json'); if (!r.ok) throw new Error('map data ' + r.status);
  const topo = await r.json();
  const countries = window.topojson.feature(topo, topo.objects.countries).features.filter((f) => f.id);
  const land = window.topojson.feature(topo, topo.objects.land);
  /* the land becomes a field of dots: paint the land once, then sample it */
  const W = 720, H = 360, cv = document.createElement('canvas'); cv.width = W; cv.height = H;
  const cx2 = cv.getContext('2d'), eq = d3.geoEquirectangular().fitSize([W, H], { type: 'Sphere' });
  cx2.fillStyle = '#fff'; cx2.beginPath(); d3.geoPath(eq, cx2)(land); cx2.fill();
  const px = cx2.getImageData(0, 0, W, H).data, dots = [];
  for (let lat = -78; lat <= 83; lat += 2.2) { const step = 2.2 / Math.max(0.18, Math.cos((lat * Math.PI) / 180)); for (let lon = -180; lon < 180; lon += step) { const p = eq([lon, lat]); if (px[(Math.round(p[1]) * W + Math.round(p[0])) * 4 + 3] > 128) dots.push([lon, lat]); } }
  const cents = {};
  countries.forEach((f) => {
    let g = f; if (f.geometry.type === 'MultiPolygon') { const best = f.geometry.coordinates.map((c) => ({ type: 'Feature', geometry: { type: 'Polygon', coordinates: c } })).sort((a, b) => d3.geoArea(b) - d3.geoArea(a))[0]; g = best; }
    cents[f.id] = d3.geoCentroid(g);
  });
  return { countries, dots, cents, names: Object.fromEntries(countries.map((f) => [f.id, f.properties.name])) };
})().catch((e) => { worldPromise = null; throw e; }));

const SAMPLE_PEOPLE = [['840', 38], ['826', 14], ['356', 41], ['566', 29], ['076', 17], ['276', 9], ['124', 12], ['036', 7], ['710', 8], ['818', 16], ['586', 22], ['608', 13], ['484', 10], ['250', 6], ['392', 5], ['410', 5], ['682', 7], ['792', 6], ['288', 11], ['404', 9]];
const SAMPLE_PARTNERS = [
  { id: '566', name: 'Amara', goal: 'Aiming for 80% accuracy, sitting Step 1 in March', streak: 21, hours: 18 },
  { id: '076', name: 'Lucas', goal: 'Cardiology block, 40 questions a day', streak: 12, hours: 15 },
  { id: '356', name: 'Priya', goal: 'Flashcards every morning, exam in January', streak: 33, hours: 24 },
  { id: '124', name: 'Noah', goal: 'Rebuilding fundamentals after a gap year', streak: 9, hours: 12 },
  { id: '036', name: 'Mia', goal: 'Weekly review of every missed question', streak: 17, hours: 20 },
];

function Globe2D({ world, marks, me, flight, onFlightEnd }) {
  const cv = useRef(null), st = useRef({ rot: [10, -18], drag: null, idle: 0, flight: null, hover: null }), prop = useRef({ marks, me });
  const [tip, setTip] = useState(null);
  prop.current = { marks, me };
  useEffect(() => {
    if (!flight || !world) return;
    const a = world.cents[flight.from], b = world.cents[flight.to]; if (!a || !b) return;
    const itp = d3.geoInterpolate(a, b), mid = itp(0.5);
    st.current.flight = { itp, a, b, t0: performance.now(), dur: 3600, rot0: st.current.rot.slice(), rot1: [-mid[0], -mid[1] * 0.8], done: false };
  }, [flight && flight.key, !!world]);
  useEffect(() => {
    if (!world || !cv.current) return undefined;
    const c = cv.current, ctx = c.getContext('2d'); let raf = 0, W = 0;
    const proj = d3.geoOrthographic().clipAngle(90).precision(0.5), path = d3.geoPath(proj, ctx);
    const fit = () => { const r = c.getBoundingClientRect(), dpr = Math.min(2, window.devicePixelRatio || 1); W = r.width; c.width = Math.round(r.width * dpr); c.height = Math.round(r.height * dpr); ctx.setTransform(dpr, 0, 0, dpr, 0, 0); proj.translate([W / 2, W / 2]).scale(W / 2 * 0.86); };
    fit(); const ro = new ResizeObserver(fit); ro.observe(c);
    const vis = (p) => d3.geoDistance(p, [-st.current.rot[0], -st.current.rot[1]]) < Math.PI / 2 - 0.04;
    const frame = (now) => {
      raf = requestAnimationFrame(frame); const s = st.current; const fl = s.flight;
      if (fl) {
        const k = Math.min(1, (now - fl.t0) / fl.dur), e = k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
        const rk = Math.min(1, k * 1.8), re = 1 - Math.pow(1 - rk, 3);
        s.rot = [fl.rot0[0] + (fl.rot1[0] - fl.rot0[0]) * re, fl.rot0[1] + (fl.rot1[1] - fl.rot0[1]) * re];
        fl.k = e; if (k >= 1 && !fl.done) { fl.done = true; fl.hold = now; }
        if (fl.done && now - fl.hold > 2600) { s.flight = null; s.idle = now; if (onFlightEnd) onFlightEnd(); }
      } else if (!s.drag && now - s.idle > 1800) s.rot = [s.rot[0] + 0.12, s.rot[1]];
      proj.rotate(s.rot);
      ctx.clearRect(0, 0, W, W);
      const R = proj.scale(), cx0 = W / 2;
      const glow = ctx.createRadialGradient(cx0, cx0, R * 0.96, cx0, cx0, R * 1.18); glow.addColorStop(0, 'rgba(255,255,255,.16)'); glow.addColorStop(1, 'rgba(255,255,255,0)');
      ctx.fillStyle = glow; ctx.beginPath(); ctx.arc(cx0, cx0, R * 1.18, 0, 7); ctx.fill();
      const body = ctx.createRadialGradient(cx0 - R * 0.35, cx0 - R * 0.4, R * 0.1, cx0, cx0, R); body.addColorStop(0, '#23232c'); body.addColorStop(1, '#08080b');
      ctx.fillStyle = body; ctx.beginPath(); path({ type: 'Sphere' }); ctx.fill();
      ctx.strokeStyle = 'rgba(255,255,255,.06)'; ctx.lineWidth = 1; ctx.beginPath(); path(d3.geoGraticule10()); ctx.stroke();
      ctx.fillStyle = 'rgba(255,255,255,.62)';
      const dr = Math.max(0.9, R / 260);
      for (let i = 0; i < world.dots.length; i++) { const d = world.dots[i]; if (!vis(d)) continue; const p = proj(d); ctx.beginPath(); ctx.arc(p[0], p[1], dr, 0, 7); ctx.fill(); }
      const rim = ctx.createRadialGradient(cx0, cx0, R * 0.8, cx0, cx0, R); rim.addColorStop(0, 'rgba(255,255,255,0)'); rim.addColorStop(1, 'rgba(255,255,255,.12)'); ctx.fillStyle = rim; ctx.beginPath(); ctx.arc(cx0, cx0, R, 0, 7); ctx.fill();
      const pulse = (Math.sin(now / 520) + 1) / 2;
      const { marks: ms, me: mine } = prop.current;
      ms.forEach((m) => {
        const g = world.cents[m.id]; if (!g || !vis(g)) return; const p = proj(g), r = 3 + Math.min(10, Math.log2(m.n + 1) * 1.8);
        ctx.fillStyle = 'rgba(255,230,0,' + (0.1 + pulse * 0.14) + ')'; ctx.beginPath(); ctx.arc(p[0], p[1], r * (2.2 + pulse * 0.8), 0, 7); ctx.fill();
        ctx.fillStyle = '#FFE600'; ctx.beginPath(); ctx.arc(p[0], p[1], r * 0.62, 0, 7); ctx.fill();
      });
      if (mine && world.cents[mine] && vis(world.cents[mine])) { const p = proj(world.cents[mine]); ctx.strokeStyle = '#fff'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(p[0], p[1], 9 + pulse * 3, 0, 7); ctx.stroke(); ctx.fillStyle = '#fff'; ctx.beginPath(); ctx.arc(p[0], p[1], 4, 0, 7); ctx.fill(); }
      if (fl && fl.k > 0) {
        const pts = []; const N = 90; for (let i = 0; i <= N * fl.k; i++) pts.push(fl.itp(i / N));
        const grad = ctx.createLinearGradient(0, 0, W, W); grad.addColorStop(0, '#fff'); grad.addColorStop(1, '#FFE600');
        ctx.lineCap = 'round'; ctx.lineWidth = 2.6; ctx.strokeStyle = grad; ctx.shadowColor = 'rgba(255,230,0,.9)'; ctx.shadowBlur = 14; ctx.beginPath(); path({ type: 'LineString', coordinates: pts }); ctx.stroke(); ctx.shadowBlur = 0;
        const head = fl.itp(fl.k); if (vis(head)) { const p = proj(head); ctx.fillStyle = '#fff'; ctx.shadowColor = '#FFE600'; ctx.shadowBlur = 20; ctx.beginPath(); ctx.arc(p[0], p[1], 5.5, 0, 7); ctx.fill(); ctx.shadowBlur = 0; }
        if (fl.done) { const q = proj(fl.b); if (vis(fl.b)) { const k2 = ((now - fl.hold) / 700) % 1; ctx.strokeStyle = 'rgba(255,255,255,' + (1 - k2) + ')'; ctx.lineWidth = 2; ctx.beginPath(); ctx.arc(q[0], q[1], 8 + k2 * 26, 0, 7); ctx.stroke(); } }
      }
    };
    raf = requestAnimationFrame(frame);
    const down = (e) => { c.setPointerCapture(e.pointerId); st.current.drag = { x: e.clientX, y: e.clientY }; st.current.flight = null; };
    const move = (e) => {
      const s = st.current;
      if (s.drag) { const dx = e.clientX - s.drag.x, dy = e.clientY - s.drag.y; s.drag = { x: e.clientX, y: e.clientY }; s.rot = [s.rot[0] + dx * 0.4, Math.max(-70, Math.min(70, s.rot[1] - dy * 0.4))]; setTip(null); return; }
      const r = c.getBoundingClientRect(), x = e.clientX - r.left, y = e.clientY - r.top; let hit = null;
      prop.current.marks.forEach((m) => { const g = world.cents[m.id]; if (!g || !vis(g)) return; const p = proj(g); if (Math.hypot(p[0] - x, p[1] - y) < 16) hit = { x, y, text: m.name + ' · ' + m.n + ' student' + (m.n === 1 ? '' : 's') }; });
      setTip(hit);
    };
    const up = () => { st.current.drag = null; st.current.idle = performance.now(); };
    c.addEventListener('pointerdown', down); c.addEventListener('pointermove', move); c.addEventListener('pointerup', up); c.addEventListener('pointercancel', up); c.addEventListener('pointerleave', () => setTip(null));
    return () => { cancelAnimationFrame(raf); ro.disconnect(); c.removeEventListener('pointerdown', down); c.removeEventListener('pointermove', move); c.removeEventListener('pointerup', up); c.removeEventListener('pointercancel', up); };
  }, [world]);
  return html`<div className="gb"><canvas ref=${cv} aria-label="A globe showing where students are studying from" role="img"></canvas>${tip && html`<span className="gb-tip" style=${{ left: tip.x, top: tip.y }}>${tip.text}</span>`}</div>`;
}

function glSupported() { try { const c = document.createElement('canvas'); return !!(c.getContext('webgl2') || c.getContext('webgl')); } catch { return false; } }
const GL_IMG = 'https://cdn.jsdelivr.net/npm/three-globe@2.31.1/example/img/';
function GlobeGL({ marks, me, flight, onFlightEnd, onFail }) {
  const host = useRef(null), gref = useRef(null), live = useRef({ marks, me });
  live.current = { marks, me };
  const feed = (g) => {
    const { marks: ms, me: mine } = live.current;
    const pts = ms.map((m) => ({ lat: m.lat, lng: m.lng, n: m.n, name: m.name, me: false }));
    if (mine) pts.push({ lat: mine[0], lng: mine[1], n: 1, name: 'You', me: true });
    g.pointsData(pts); g.ringsData(pts.slice(0, 40));
  };
  useEffect(() => {
    let dead = false, ro = null;
    loadScript('https://cdn.jsdelivr.net/npm/globe.gl@2.32.3/dist/globe.gl.min.js').then(() => {
      if (dead || !host.current) return;
      const el = host.current, W = el.clientWidth || 600;
      const g = window.Globe()(el).width(W).height(W)
        .backgroundColor('rgba(0,0,0,0)').backgroundImageUrl(GL_IMG + 'night-sky.png')
        .globeImageUrl(GL_IMG + 'earth-blue-marble.jpg').bumpImageUrl(GL_IMG + 'earth-topology.png')
        .showAtmosphere(true).atmosphereColor('#8fbaff').atmosphereAltitude(0.3)
        .pointLat('lat').pointLng('lng').pointColor((d) => (d.me ? '#ffffff' : '#ffd400')).pointAltitude((d) => 0.004 + Math.log2(d.n + 1) * 0.003).pointRadius((d) => (d.me ? 0.9 : 0.5 + Math.min(1.2, Math.log2(d.n + 1) * 0.14)))
        .pointLabel((d) => '<div class="gl-lab"><b>' + d.name + '</b>' + (d.me ? '' : '<br>' + d.n + ' student' + (d.n === 1 ? '' : 's')) + '</div>')
        .ringLat('lat').ringLng('lng').ringColor((d) => (t) => (d.me ? 'rgba(255,255,255,' + (1 - t) + ')' : 'rgba(255,212,0,' + (1 - t) * 0.8 + ')')).ringMaxRadius(3.2).ringPropagationSpeed(1.4).ringRepeatPeriod(1700)
        .arcColor(() => ['#ffffff', '#ffd400']).arcStroke(0.55).arcDashLength(0.45).arcDashGap(0.12).arcDashAnimateTime(1700).arcAltitudeAutoScale(0.5).arcsTransitionDuration(0);
      const ctl = g.controls(); ctl.autoRotate = true; ctl.autoRotateSpeed = 0.45; ctl.enableZoom = false; ctl.enablePan = false;
      g.pointOfView({ lat: 18, lng: 10, altitude: 2.35 }, 0);
      gref.current = g; feed(g);
      ro = new ResizeObserver(() => { const w = el.clientWidth; if (w) g.width(w).height(w); }); ro.observe(el);
    }).catch(() => onFail && onFail());
    return () => { dead = true; if (ro) ro.disconnect(); if (gref.current && gref.current._destructor) gref.current._destructor(); gref.current = null; if (host.current) host.current.innerHTML = ''; };
  }, []);
  useEffect(() => { if (gref.current) feed(gref.current); }, [marks, me]);
  useEffect(() => {
    const g = gref.current; if (!g || !flight) return undefined;
    const ctl = g.controls(); ctl.autoRotate = false;
    const A = flight.fromLL, B = flight.toLL, mid = [(A[0] + B[0]) / 2, (A[1] + B[1]) / 2];
    g.pointOfView({ lat: mid[0], lng: Math.abs(A[1] - B[1]) > 180 ? mid[1] + 180 : mid[1], altitude: 2.1 }, 1500);
    g.arcsData([{ startLat: A[0], startLng: A[1], endLat: B[0], endLng: B[1] }]);
    const t = setTimeout(() => { g.arcsData([]); ctl.autoRotate = true; if (onFlightEnd) onFlightEnd(); }, 6200);
    return () => { clearTimeout(t); };
  }, [flight && flight.key]);
  const zoom = (d) => { const g = gref.current; if (!g) return; const p = g.pointOfView(); g.pointOfView({ altitude: Math.max(1.2, Math.min(6.5, p.altitude + d)) }, 700); };
  return html`<div className="gb gl3"><div ref=${host} className="gl3-host"></div><div className="gl3-zoom"><button onClick=${() => zoom(-0.6)} aria-label="Zoom in"><${Icon} n="fa-plus" /></button><button onClick=${() => zoom(0.9)} aria-label="Zoom out"><${Icon} n="fa-minus" /></button></div></div>`;
}

function CountryPicker({ world, value, onPick }) {
  const [q, setQ] = useState(''), [open, setOpen] = useState(false);
  const list = useMemo(() => (world ? world.countries.map((f) => [f.id, f.properties.name]).sort((a, b) => a[1].localeCompare(b[1])) : []), [world]);
  const hits = list.filter(([, n]) => n.toLowerCase().includes(q.trim().toLowerCase())).slice(0, 7);
  return html`<div className="cp"><label className=${cx('cp-in', open && 'open')}><${Icon} n="fa-magnifying-glass" /><input value=${open ? q : (value && world ? world.names[value] || '' : '')} placeholder="Search your country" onFocus=${() => { setOpen(true); setQ(''); }} onChange=${(e) => setQ(e.target.value)} onBlur=${() => setTimeout(() => setOpen(false), 150)} /></label>
    ${open && html`<ul className="cp-list">${hits.length ? hits.map(([id, n]) => html`<li key=${id}><button type="button" onMouseDown=${(e) => e.preventDefault()} onClick=${() => { onPick(id, n); setOpen(false); }}>${n}${id === value && html`<${Icon} n="fa-check" />`}</button></li>`) : html`<li className="cp-none">No match</li>`}</ul>`}</div>`;
}

function Community({ go }) {
  const c = useCloud();
  const [world, setWorld] = useState(null), [err, setErr] = useState('');
  const [rows, setRows] = useState([]), [mine, setMine] = useState(undefined);
  const [sample, setSample] = useState(true);
  const [busy, setBusy] = useState(false);
  const [flight, setFlight] = useState(null), [partner, setPartner] = useState(null);
  const [gl, setGl] = useState(() => glSupported());
  useEffect(() => { loadWorld().then(setWorld).catch((e) => setErr(String(e.message || e))); }, []);
  const refresh = useCallback(async () => {
    if (!sb || !CloudStore.get().user) { setRows([]); setMine(null); return; }
    const a = await sb.rpc('community_map', { min_count: 3 }); if (!a.error) setRows(a.data || []);
    let b = await sb.from('presence').select('country_id,country_name,region,lat,lng').eq('user_id', CloudStore.get().user.id).maybeSingle();
    if (b.error) b = await sb.from('presence').select('country_id,country_name').eq('user_id', CloudStore.get().user.id).maybeSingle();
    setMine(b.data || null);
  }, []);
  useEffect(() => { refresh(); }, [c.user && c.user.id]);
  const save = async (id, name) => { setBusy(true); const { error } = await sb.from('presence').upsert({ user_id: c.user.id, country_id: id, country_name: name, region: null, lat: null, lng: null }); setBusy(false); if (error) toast('Could not save. ' + error.message); else { toast('You are on the globe, as ' + name + ' only.'); refresh(); } };
  const remove = async () => { setBusy(true); await leaveGlobe(c.user); PrefsStore.set({ ...PrefsStore.get(), globe: false }); setBusy(false); setMine(null); toast('You are off the globe.'); refresh(); };
  const auto = async () => { setBusy(true); try { await placeMe(c.user, false); PrefsStore.set({ ...PrefsStore.get(), globe: true }); refresh(); } catch { toast('Could not work out your location. You can pick your country instead.'); } setBusy(false); };
  const ll = (id) => { const g = world && world.cents[id]; return g ? [g[1], g[0]] : [0, 0]; };
  const place = (r) => (r.lat != null && r.lng != null ? [r.lat, r.lng] : ll(r.country_id || r.id));
  const real = rows.map((r) => { const p = place(r); return { id: r.country_id, n: Number(r.n), name: (r.region ? r.region + ', ' : '') + r.country_name, lat: p[0], lng: p[1] }; });
  const fake = sample && world ? SAMPLE_PEOPLE.map(([id, n]) => { const p = ll(id); return { id, n, name: world.names[id] || id, lat: p[0], lng: p[1] }; }) : [];
  const onGlobe = !!(c.user && mine);
  const marks = onGlobe ? [...real, ...fake.filter((f) => !real.some((r) => r.id === f.id))] : [];
  const total = marks.reduce((s, m) => s + m.n, 0);
  const myLL = onGlobe ? place(mine) : null;
  const match = () => {
    if (!mine) { toast('Join the globe first, and the match will fly from your place.'); return; }
    const p = SAMPLE_PARTNERS[Math.floor(Math.random() * SAMPLE_PARTNERS.length)];
    setPartner(null); setFlight({ from: mine.country_id, to: p.id, fromLL: myLL, toLL: ll(p.id), key: Date.now() }); setTimeout(() => setPartner(p), 2600);
  };
  const where = mine ? (mine.region ? mine.region + ', ' : '') + mine.country_name : '';
  const dm = /^#\/u\/([A-Za-z0-9_]{3,20})/.exec(location.hash);
  return html`<section className="cm" data-nav="dark"><${ConnectLayer} deepUser=${dm ? dm[1] : null} />
    <div className="cm-head"><p className="eyebrow">Community</p><h1>Students, <span className="hl">everywhere.</span></h1>
      <p>A quiet look at where people are studying from. It works both ways: you see others only while you are on the globe too. Only a country and state are counted, never an address, and you can leave whenever you like.</p></div>
    <div className="cm-grid">
      <div className="cm-globe">${gl ? html`<${GlobeGL} marks=${marks} me=${myLL} flight=${flight} onFlightEnd=${() => { setFlight(null); setPartner(null); }} onFail=${() => setGl(false)} />` : world ? html`<${Globe2D} world=${world} marks=${marks} me=${mine && mine.country_id} flight=${flight} onFlightEnd=${() => { setFlight(null); setPartner(null); }} />` : html`<div className="cm-load">${err ? 'The map could not load. Check your connection and refresh.' : html`<span></span>Loading the globe…`}</div>`}
        <div className="cm-legend"><i></i> ${!onGlobe ? 'Turn on the globe to see where students are' : total ? total + ' student' + (total === 1 ? '' : 's') + ' in ' + marks.length + ' place' + (marks.length === 1 ? '' : 's') : 'No one on the map yet'}${onGlobe && sample && fake.length ? ' · includes sample data' : ''}</div></div>
      <div className="cm-side">
        <div className="cm-card"><small>Your place on the globe</small>
          ${!c.ready || (c.user && mine === undefined) ? html`<p className="cm-skel">Checking your place on the globe…</p>` : !c.user ? html`<p>Sign in to be counted automatically, and to see real counts. You choose at sign-up and can change it any time.</p><button className="lh-btn" onClick=${() => patchCloud({ modal: true })}><${Icon} n="fa-user" /> Sign in</button>` : html`<${Fragment}>
            <p>${mine ? html`You are counted in <b>${where}</b>. Others only ever see totals, and places with fewer than three students stay hidden.` : 'You are not on the globe. Add yourself from your connection, or pick a country by hand.'}</p>
            ${!mine && html`<button className="lh-btn" onClick=${auto} disabled=${busy}><${Icon} n="fa-location-dot" /> ${busy ? 'Working it out…' : 'Add me automatically'}</button>`}
            <${CountryPicker} world=${world} value=${mine && mine.country_id} onPick=${save} />
            ${mine && html`<button className="cm-off" onClick=${remove} disabled=${busy}><${Icon} n="fa-eye-slash" /> Take me off the globe</button>`}<//>`}</div>
        <div className="cm-card dark"><small>Find a study partner</small>
          <p>Soon, you will be matched with someone who shares your goals and keeps the same steady streak. Here is a preview with sample students.</p>
          <button className="lh-btn" onClick=${match} disabled=${!world}><${Icon} n="fa-wand-magic-sparkles" /> Preview a match</button>
          ${partner && html`<div className="cm-match"><b>${partner.name}</b><span>${world.names[partner.id]}</span><p>${partner.goal}</p><div><em>${partner.streak} day streak</em><em>${partner.hours} h a week</em></div><small>Sample student. Real matching is coming.</small></div>`}</div>
        ${onGlobe && html`<label className="cm-check"><input type="checkbox" checked=${sample} onChange=${(e) => setSample(e.target.checked)} /><span>Show sample students until the community grows</span></label>`}
      </div>
    </div>
  </section>`;
}

/* ==========================================================================
   CONNECT: matches, search, requests, chat and a shareable QR code.
   Opt-in only. Matching uses a small summary, never your raw history.
   ========================================================================== */
const hueOf = (s) => { let h = 0; for (const ch of s || '?') h = (h * 31 + ch.charCodeAt(0)) % 360; return h; };
function Av({ p, cls }) {
  const nm = p.display_name || p.name || p.username || '?';
  return p.avatar_url ? html`<img className=${cls} src=${p.avatar_url} alt="" onError=${hidePicture} />` : html`<span className=${cx('cn-ph', cls)} style=${{ '--h': hueOf(p.username || nm) }}>${nm.charAt(0).toUpperCase()}</span>`;
}
function myStats() {
  const attempts = window.AttemptStore.get().filter((a) => !a.sample), log = FocusLog.get(), plan = PlanStore.get(), goals = GoalsStore.get();
  const week = Math.round(log.filter((f) => Date.now() - f.at < 7 * DAY).reduce((s, f) => s + f.sec, 0) / 60);
  const last = attempts.slice(-10), acc = last.length ? Math.round(last.reduce((s, a) => s + pctOf(a), 0) / last.length) : 0;
  return { weekly_minutes: Math.min(10080, week), accuracy: acc, streak: Math.min(3650, summarize(attempts).streak), exam_date: plan.exam || null, goal_accuracy: goals.accuracy || null, weekly_goal_hours: goals.weeklyHours || null };
}
let pubTimer = 0;
async function publishStats(force) {
  const c = CloudStore.get(); if (!c.user || !(c.profile && c.profile.discoverable)) return;
  const st = myStats(), sig = JSON.stringify(st); let last = {}; try { last = JSON.parse(localStorage.getItem('usmle-site:pub') || '{}'); } catch { /* ignore */ }
  if (!force && last.sig === sig && last.uid === c.user.id) return;                                         /* nothing changed: no write */
  if (!force && last.uid === c.user.id && Date.now() - (last.t || 0) < 300000) { clearTimeout(pubTimer); pubTimer = setTimeout(() => publishStats().catch(() => {}), 300000 - (Date.now() - last.t)); return; }   /* at most one write per 5 minutes */
  try { localStorage.setItem('usmle-site:pub', JSON.stringify({ sig, t: Date.now(), uid: c.user.id })); } catch { /* ignore */ }
  await sb.from('public_stats').upsert({ user_id: c.user.id, ...st, updated_at: new Date().toISOString() });
}
async function setDiscoverable(on) {
  const c = CloudStore.get(); if (!c.user) return false;
  const { error } = await sb.from('profiles').update({ discoverable: on }).eq('id', c.user.id);
  if (error) { toast(/discoverable/i.test(error.message) ? 'Matching is not switched on for this project yet.' : error.message); return false; }
  patchCloud({ profile: { ...c.profile, discoverable: on } });
  if (on) await publishStats(true); else await sb.from('public_stats').delete().eq('user_id', c.user.id);
  return true;
}
const shareLink = (u) => location.href.split('#')[0] + '#/u/' + u;
function whyChips(p, me) {
  const out = []; const mine = me || myStats();
  const hrs = (m) => Math.round((m / 60) * 10) / 10;
  if (p.weekly_minutes != null && Math.abs(p.weekly_minutes - mine.weekly_minutes) <= Math.max(60, 0.25 * Math.max(p.weekly_minutes, mine.weekly_minutes))) out.push('Studies about ' + hrs(p.weekly_minutes) + ' h a week');
  if (p.accuracy != null && Math.abs(p.accuracy - mine.accuracy) <= 8) out.push('Scores near ' + p.accuracy + '%');
  if (p.exam_date && mine.exam_date && Math.abs((new Date(p.exam_date) - new Date(mine.exam_date)) / DAY) <= 45) out.push('Exam in ' + new Date(p.exam_date).toLocaleDateString(undefined, { month: 'long' }));
  if (p.streak != null && Math.abs(p.streak - mine.streak) <= 5) out.push('Steady ' + p.streak + ' day streak');
  if (p.goal_accuracy != null && mine.goal_accuracy && Math.abs(p.goal_accuracy - mine.goal_accuracy) <= 5) out.push('Aiming for ' + p.goal_accuracy + '%');
  return out.slice(0, 3);
}
const SAMPLE_MATCHES = [
  { name: 'Kai', un: 'kai_inks', st: 'Step 1', yr: 'MS2', c: 'United States', sc: 94, wm: 1080, ac: 78, sk: 21, bio: 'Cardiology block, flashcards every morning, tattoos by night.', ph: 0 },
  { name: 'Zuri', un: 'zuri_md', st: 'Step 1', yr: 'MS2', c: 'Nigeria', sc: 92, wm: 960, ac: 76, sk: 17, bio: 'Quiet mornings, loud study playlists.', ph: 1 },
  { name: 'Imani', un: 'imani_r', st: 'Step 2 CK', yr: 'MS3', c: 'United Kingdom', sc: 90, wm: 1440, ac: 81, sk: 33, bio: 'Reviewing every missed question, weekly.', ph: 2 },
  { name: 'David', un: 'david_o', st: 'Step 1', yr: 'MS2', c: 'Ghana', sc: 88, wm: 900, ac: 74, sk: 12, bio: 'Forty questions a day and a long walk after.', ph: 3 },
  { name: 'Jalen', un: 'jalen_s', st: 'Step 1', yr: 'Gap year', c: 'United States', sc: 86, wm: 720, ac: 70, sk: 9, bio: 'Rebuilding fundamentals, one system at a time.', ph: 4 },
  { name: 'Biscuit', un: 'biscuit_pup', st: 'Step 3', yr: 'MS4', c: 'Canada', sc: 84, wm: 600, ac: 66, sk: 5, bio: 'Studies best by the lake. Very good boy.', ph: 5 },
  { name: 'Arman', un: 'arman_k', st: 'Step 1', yr: 'MS1', c: 'India', sc: 83, wm: 840, ac: 72, sk: 14, bio: 'Collects toy cars, memorises pathways.', ph: 6 },
  { name: 'Ngozi', un: 'ngozi_a', st: 'Step 2 CK', yr: 'MS3', c: 'Nigeria', sc: 81, wm: 1020, ac: 77, sk: 19, bio: 'Biostats is my villain origin story.', ph: 7 },
  { name: 'Mateo', un: 'mateo_l', st: 'Step 1', yr: 'MS2', c: 'Brazil', sc: 79, wm: 780, ac: 73, sk: 11, bio: 'Anki, coffee, repeat.', ph: 8 },
].map((x, i) => ({ sample: true, user_id: 's' + i, step: x.st, display_name: x.name, username: x.un, study_year: x.yr, country_name: x.c, score: x.sc, weekly_minutes: x.wm, accuracy: x.ac, streak: x.sk, goal_accuracy: 80, bio: x.bio, avatar_url: SAMPLE_PHOTOS[x.ph] }));

const SAMPLE_FRIENDS = [
  { about: "Second-year student, quietly obsessed with pathology. I read manga on the train, and I rebuild every wrong answer into a flashcard before bed. Weekends are for long slow walks and a full block of UWorld. I study best with someone to explain things to, so message me if you want a partner for Step 1.", name: 'Idris', un: 'idris_v', st: 'Step 1', yr: 'MS2', c: 'United Kingdom', bio: 'Vagabond rereader. Pathology on weekends.', ph: 0, last: 'That mnemonic saved me today 😅', unread: 2, online: true },
  { about: "I keep my study life simple: biochemistry in the morning, practice questions after lunch, a run when my head gets loud. I wear too many beads and I write mnemonics that make no sense to anyone else. Always up for a Zoom study hour, camera optional, snacks mandatory.", name: 'Rafael', un: 'rafa_b', st: 'Step 1', yr: 'MS1', c: 'Brazil', bio: 'Beads, biochem, breathing.', ph: 1, last: 'See you on Zoom at 7?', unread: 0, online: true },
  { about: "Matcha, flashcards and a spotless calendar. I am preparing for Step 2 CK and I share every deck I build, so ask and I will send it over. I learn through teaching, so I will happily quiz you on anything from OB-GYN to biostats.", name: 'Nia', un: 'nia_m', st: 'Step 2 CK', yr: 'MS3', c: 'United States', bio: 'Matcha and MCQs.', ph: 2, last: 'Sent you the OB-GYN deck', unread: 0, online: false },
  { about: "Trauma rotation taught me patience and a lot about coffee. I review in short sharp sprints, then step outside to reset. I am looking for steady study friends who show up on the hard days too, not only the good ones.", name: 'Kweku', un: 'kweku_a', st: 'Step 1', yr: 'MS2', c: 'Ghana', bio: 'Trauma rotation taught me patience.', ph: 3, last: 'Typing…', unread: 0, online: true, typing: true },
  { about: "Walks, audio decks and a lot of repetition. I study in Portuguese and English and I switch when a concept refuses to land. I like quiet partners, shared goals and a weekly check-in about what is actually working.", name: 'Diego', un: 'diego_s', st: 'Step 1', yr: 'MS2', c: 'Brazil', bio: 'Walks and audio decks.', ph: 4, last: 'Thanks, I needed that reminder.', unread: 0, online: false },
  { about: "Rings, rhythm and rounds. I am deep in my clinical years and I turn every patient I meet into a question I can answer later. I keep a streak, I forgive myself when I break it, and I start again the next morning.", name: 'Chidi', un: 'chidi_h', st: 'Step 2 CK', yr: 'MS4', c: 'Nigeria', bio: 'Rings, rhythm, rounds.', ph: 5, last: 'Lol same here', unread: 1, online: true },
  { about: "Gym at six, questions at seven. I treat studying like training: a plan, a log, and a rest day I actually take. Happy to swap schedules, hold you accountable, and celebrate every score that moves.", name: 'Marcus', un: 'marcus_t', st: 'Step 1', yr: 'MS2', c: 'United States', bio: 'Gym at 6, questions at 7.', ph: 6, last: 'You up?', unread: 0, online: false },
].map((x, i) => ({ sample: true, conn_id: 'f' + i, other_id: 'f' + i, user_id: 'f' + i, status: 'accepted', display_name: x.name, username: x.un, step: x.st, study_year: x.yr, country_name: x.c, bio: x.bio, about: x.about, avatar_url: FRIEND_PHOTOS[x.ph], last_body: x.last, last_at: Date.now() - [30e3, 2 * 3600e3, 26 * 3600e3, 30 * 3600e3, 5 * 86400e3, 20 * 86400e3, 40 * 86400e3][i], unread: x.unread, online: x.online, typing: x.typing }));


/* inline icons for the connect layer (no font dependency) */
const CI = {
  compass: 'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM15.6 8.4l-2 5.2-5.2 2 2-5.2z',
  search: 'M11 4a7 7 0 1 0 0 14 7 7 0 0 0 0-14zM20 20l-4-4',
  people: 'M9 11a3.5 3.5 0 1 0 0-7 3.5 3.5 0 0 0 0 7zM2.5 20c.4-3.6 3.2-5.6 6.5-5.6s6.1 2 6.5 5.6M16.5 4.4a3.5 3.5 0 0 1 0 6.6M18 14.6c2 .6 3.3 2.4 3.5 5.4',
  qr: 'M4 4h6v6H4zM14 4h6v6h-6zM4 14h6v6H4zM14 14h2v2h-2zM18 14h2v2M14 18h2v2M18 18h2v2M7 7h.01M17 7h.01M7 17h.01',
  link: 'M10 14a4 4 0 0 0 5.7 0l3.6-3.6a4 4 0 0 0-5.7-5.7L12.4 5.9M14 10a4 4 0 0 0-5.7 0l-3.6 3.6a4 4 0 0 0 5.7 5.7l1.2-1.2',
  x: 'M6 6l12 12M18 6L6 18',
  pin: 'M12 21s7-6.2 7-11.5A7 7 0 0 0 5 9.5C5 14.800 12 21 12 21zM12 12a2.500 2.500 0 1 0 0-5 2.500 2.500 0 0 0 0 5z',
  back: 'M19 12H5M11 6l-6 6 6 6',
  chev: 'M6 9l6 6 6-6',
  chat: 'M4 12a7 7 0 0 1 7-7h2a7 7 0 0 1 7 7v0a7 7 0 0 1-7 7H8l-4 3v-5a7 7 0 0 1 0-5z',
  send: 'M4 12l16-8-6 18-3-7-7-3z',
  mic: 'M12 3a3 3 0 0 1 3 3v6a3 3 0 1 1-6 0V6a3 3 0 0 1 3-3zM5 11a7 7 0 0 0 14 0M12 18v3',
  plus: 'M12 5v14M5 12h14',
  smile: 'M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM8.5 10h.01M15.5 10h.01M8 14c1 1.5 2.5 2.3 4 2.3S15 15.5 16 14',
  phone: 'M5 4h4l2 5-2.5 1.5a11 11 0 0 0 5 5L15 13l5 2v4a2 2 0 0 1-2 2A16 16 0 0 1 3 6a2 2 0 0 1 2-2z',
  more: 'M12 6h.01M12 12h.01M12 18h.01',
};
const CIcon = ({ n, size = 22, sw = 1.9 }) => html`<svg className="ci" viewBox="0 0 24 24" width=${size} height=${size} fill="none" stroke="currentColor" strokeWidth=${sw} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d=${CI[n]} /></svg>`;

/* ---------- QR code for sharing your profile ---------- */
function QrCanvas({ text, size = 260 }) {
  const ref = useRef(null);
  useEffect(() => {
    let dead = false;
    loadScript('https://cdn.jsdelivr.net/npm/qrcode-generator@1.4.4/qrcode.js').then(() => {
      if (dead || !ref.current) return;
      const q = window.qrcode(0, 'M'); q.addData(text); q.make();
      const n = q.getModuleCount(), cell = Math.floor(size / (n + 4)), pad = Math.floor((size - cell * n) / 2), cv = ref.current;
      cv.width = cv.height = size; const g = cv.getContext('2d'); g.fillStyle = '#fff'; g.fillRect(0, 0, size, size); g.fillStyle = '#0b0b0d';
      for (let r = 0; r < n; r++) for (let k = 0; k < n; k++) if (q.isDark(r, k)) { const x = pad + k * cell, y = pad + r * cell, rad = cell * 0.16; g.beginPath(); g.roundRect ? g.roundRect(x, y, cell + 0.4, cell + 0.4, rad) : g.rect(x, y, cell + 0.4, cell + 0.4); g.fill(); }
    }).catch(() => {});
    return () => { dead = true; };
  }, [text]);
  return html`<canvas ref=${ref} className="qr-cv" width=${size} height=${size} aria-label="QR code for your profile" role="img"></canvas>`;
}
function QrCard({ username }) {
  const link = username ? shareLink(username) : '';
  const hosted = /^https?:$/.test(location.protocol);
  const save = () => { const cv = document.querySelector('.qr-cv'); if (!cv) return; const a = document.createElement('a'); a.href = cv.toDataURL('image/png'); a.download = 'step1-' + username + '-qr.png'; a.click(); };
  if (!username) return html`<div className="cn-empty"><p>Choose a username first, and your code appears here.</p><button className="cn-btn" onClick=${() => patchCloud({ profileOpen: true })}>Choose a username</button></div>`;
  return html`<div className="qr-card"><div className="qr-box"><${QrCanvas} text=${link} /></div><b>@${username}</b><small>${hosted ? 'Anyone who scans this opens your profile and can send a request.' : 'This only works once the site is hosted at a web address. Right now the link points to a file on your computer.'}</small>
    <div className="qr-act"><button className="cn-btn" onClick=${() => copyText(link, 'Link copied')}><${CIcon} n="link" size=${18} /> Copy link</button><button className="cn-btn ghost" onClick=${save}><${Icon} n="fa-download" /> Save image</button></div></div>`;
}

/* ---------- chat ---------- */
function Chat({ conn, onBack }) {
  const c = useCloud(), [msgs, setMsgs] = useState([]), [text, setText] = useState(''), [emoji, setEmoji] = useState(false), box = useRef(null), inp = useRef(null);
  const sample = !!conn.sample;
  useEffect(() => {
    if (sample) { setMsgs([
      { id: 'm1', sender: 'them', body: 'Hey! How are the renal flashcards going?', created_at: new Date(Date.now() - 2 * 3600000).toISOString() },
      { id: 'm2', sender: 'me', body: 'Finally clicking. Loop of Henle is still a mess though 😩', created_at: new Date(Date.now() - 2 * 3600000 + 120000).toISOString() },
      { id: 'm3', sender: 'them', body: 'Try the Sketchy video on it, saved my life.', created_at: new Date(Date.now() - 90 * 60000).toISOString() },
      { id: 'm4', sender: 'them', body: conn.last_body || 'See you later!', created_at: new Date(Date.now() - 60 * 60000).toISOString() },
    ]); return undefined; }
    let dead = false; let ch = null;
    sb.from('messages').select('id,sender,body,created_at').eq('conn_id', conn.conn_id).order('created_at', { ascending: true }).limit(300).then(({ data }) => { if (!dead && data) setMsgs(data); });
    try { ch = sb.channel('chat-' + conn.conn_id).on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'messages', filter: 'conn_id=eq.' + conn.conn_id }, (pl) => { setMsgs((m) => (m.some((x) => x.id === pl.new.id) ? m : [...m, pl.new])); }).subscribe(); } catch { /* realtime unavailable */ }
    return () => { dead = true; if (ch) sb.removeChannel(ch); };
  }, [conn.conn_id]);
  useEffect(() => { if (box.current) box.current.scrollTop = box.current.scrollHeight; }, [msgs.length]);
  const send = async (e) => {
    e && e.preventDefault(); const body = text.trim(); if (!body) return; setText(''); setEmoji(false);
    if (sample) { setMsgs((m) => [...m, { id: 'me-' + Date.now(), sender: 'me', body, created_at: new Date().toISOString() }]); setTimeout(() => setMsgs((m) => [...m, { id: 't-' + Date.now(), sender: 'them', body: 'Sample chat — real replies happen once you connect with a real student.', created_at: new Date().toISOString() }]), 900); return; }
    const { data, error } = await sb.from('messages').insert({ conn_id: conn.conn_id, sender: c.user.id, body }).select('id,sender,body,created_at').single();
    if (error) { toast('Could not send that message.'); setText(body); } else setMsgs((m) => (m.some((x) => x.id === data.id) ? m : [...m, data]));
  };
  const pickEmoji = (e) => { setText((t) => t + e); if (inp.current) inp.current.focus(); };
  const dayLabel = (iso) => { const d = new Date(iso), now = new Date(); if (d.toDateString() === now.toDateString()) return 'Today'; const y = new Date(now); y.setDate(y.getDate() - 1); if (d.toDateString() === y.toDateString()) return 'Yesterday'; return d.toLocaleDateString(undefined, { weekday: 'long', month: 'short', day: 'numeric' }); };
  const items = []; let lastDay = ''; msgs.forEach((m) => { const d = dayLabel(m.created_at); if (d !== lastDay) { items.push({ divider: d, id: 'd' + d + m.id }); lastDay = d; } items.push(m); });
  const mine = (m) => m.sender === (c.user ? c.user.id : 'me') || m.sender === 'me';
  return html`<div className="cn-chat"><div className="cn-chat-top">
      <button className="cn-back" onClick=${onBack} aria-label="Back"><${CIcon} n="back" size=${20} /></button>
      <span className="pp-ava"><${Av} p=${conn} cls="ct-av" />${conn.online && html`<i className="pp-dot lg"></i>`}</span>
      <div className="ct-nm"><b>${conn.display_name}</b><small>${conn.online ? 'Active now' : conn.typing ? 'Typing…' : '@' + conn.username}</small></div>
      <button type="button" className="ct-ic" aria-label="Call" onClick=${() => toast('Calls are coming soon.')}><${CIcon} n="phone" size=${20} /></button>
      <button type="button" className="ct-ic" aria-label="More" onClick=${() => toast('Mute, block and report are coming soon.')}><${CIcon} n="more" size=${20} sw=${3} /></button>
    </div>
    <div className="cn-msgs" ref=${box}>${msgs.length === 0 && html`<p className="cn-hint">Say hello. Share a tip, a plan, or a question you are stuck on.</p>`}
      ${items.map((m) => m.divider ? html`<div key=${m.id} className="ct-day"><span>${m.divider}</span></div>` : html`<div key=${m.id} className=${cx('cn-row2', mine(m) && 'me')}>${!mine(m) && html`<${Av} p=${conn} cls="ct-mav" />`}<div className=${cx('cn-msg', mine(m) && 'me')}><span>${m.body}</span><i>${new Date(m.created_at).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })}${mine(m) ? ' · seen' : ''}</i></div></div>`)}
      ${sample && conn.typing && html`<div className="cn-msg typing"><i className="pp-wave"><em></em><em></em><em></em></i></div>`}
    </div>
    ${emoji && html`<div className="ct-emo">${['👋','😀','😂','🥹','🙌','🔥','💪','📚','🧠','☕️','🎯','✨','😩','🫶','👀','🚀','💯','✅','❓','🎉','👏','🤝','😴','🤔','😅','🥺','😌','🙏'].map((e) => html`<button key=${e} type="button" onClick=${() => pickEmoji(e)}>${e}</button>`)}</div>`}
    <form className="cn-send" onSubmit=${send}>
      <button type="button" className="ct-tool" aria-label="Attach" onClick=${() => toast('Attachments are coming soon.')}><${CIcon} n="plus" size=${22} sw=${2.4} /></button>
      <button type="button" className=${cx('ct-tool', emoji && 'on')} aria-label="Emoji" onClick=${() => setEmoji((v) => !v)}><${CIcon} n="smile" size=${22} sw=${1.9} /></button>
      <input ref=${inp} value=${text} onChange=${(e) => setText(e.target.value)} placeholder="Write a message" maxLength="2000" aria-label="Message" />
      ${text.trim() ? html`<button type="submit" aria-label="Send"><${CIcon} n="send" size=${22} sw=${2} /></button>` : html`<button type="button" aria-label="Voice note" onClick=${() => toast('Voice notes are coming soon.')}><${CIcon} n="mic" size=${22} /></button>`}
    </form></div>`;
}

/* ---------- the profile sheet ---------- */
function PeekSheet({ p, state, onBack, onConnect }) {
  const hrs = p.weekly_minutes != null ? Math.round((p.weekly_minutes / 60) * 10) / 10 : null;
  const why = whyChips(p);
  return html`<div className="cn-sheet"><div className="cn-hero-card"><${Av} p=${p} cls="cn-hero-av" /><button className="cn-back" onClick=${onBack} aria-label="Back"><${CIcon} n="back" size=${18} /></button>
      ${p.score != null && html`<span className="cn-score-pill">${p.score}% match</span>`}</div>
    <div className="cn-sheet-body"><small className="cn-lab">${p.sample ? 'SAMPLE STUDENT' : 'STUDENT'}</small><h2>${p.display_name}</h2><p className="cn-meta">@${p.username}${p.step ? ' · ' + p.step : ''}${p.study_year ? ' · ' + p.study_year : ''}${p.country_name ? ' · ' + p.country_name : ''}</p>
      ${why.length > 0 && html`<div className="cn-chips">${why.map((w) => html`<span key=${w}>${w}</span>`)}</div>`}
      <div className="cn-stats">${[['Hours a week', hrs != null ? hrs : '–'], ['Accuracy', p.accuracy != null ? p.accuracy + '%' : '–'], ['Streak', p.streak != null ? p.streak + 'd' : '–'], ['Exam', p.exam_date ? new Date(p.exam_date).toLocaleDateString(undefined, { month: 'short', year: '2-digit' }) : '–']].map(([k, v]) => html`<div key=${k}><b>${v}</b><span>${k}</span></div>`)}</div>
      ${p.bio && html`<div className="cn-about"><small>ABOUT</small><p>${p.bio}</p></div>`}
      <button className="cn-cta" disabled=${state === 'sent' || state === 'connected'} onClick=${() => onConnect(p)}>${state === 'sent' ? 'Request sent' : state === 'connected' ? 'Connected' : 'Connect'} <${Icon} n="fa-arrow-right" /></button></div></div>`;
}

function MatchDeck({ items, onLink, onOpen }) {
  const [i, setI] = useState(0), [dx, setDx] = useState(0), [fly, setFly] = useState(''), [dragging, setDragging] = useState(false);
  const drag = useRef(null);
  useEffect(() => { setI(0); setDx(0); setFly(''); }, [items.length && items[0] && items[0].user_id]);
  const act = useCallback((kind) => { if (fly || !items[i]) return; setFly(kind); if (kind === 'link') onLink(items[i]); setTimeout(() => { setFly(''); setDx(0); setI((n) => n + 1); }, 220); }, [fly, i, items, onLink]);
  useEffect(() => { const k = (e) => { if (/INPUT|TEXTAREA/.test(e.target.tagName)) return; if (e.key === 'ArrowRight') act('link'); else if (e.key === 'ArrowLeft') act('skip'); }; window.addEventListener('keydown', k); return () => window.removeEventListener('keydown', k); }, [act]);
  const down = (e) => { if (fly || e.target.closest('button')) return; drag.current = { x: e.clientX, moved: false }; setDragging(true); e.currentTarget.setPointerCapture(e.pointerId); };
  const move = (e) => { if (!drag.current) return; const d = e.clientX - drag.current.x; if (Math.abs(d) > 6) drag.current.moved = true; setDx(d); };
  const up = () => { if (!drag.current) return; const { moved } = drag.current; drag.current = null; setDragging(false); if (dx > 65) act('link'); else if (dx < -65) act('skip'); else { setDx(0); if (!moved && items[i]) onOpen(items[i]); } };
  if (i >= items.length) return html`<div className="cn-gate"><b>That is everyone for now</b><p>You have been through all your matches. Check back as more students join, or search a username.</p><button className="cn-btn" onClick=${() => setI(0)}>Start again</button></div>`;
  const shown = [items[i], items[i + 1], items[i + 2]].filter(Boolean);
  const pos = (k) => (k === 0 ? 'c' : k === 1 ? 'r' : 'l');
  const cur = fly === 'link' ? 640 : fly === 'skip' ? -640 : dx;
  const hrs = (m) => (m ? Math.round(m / 60) + 'h a week' : '');
  return html`<div className="mc-deck"><div className="mc-stage">
      ${shown.map((p, k) => { const c0 = k === 0; return html`<div key=${p.user_id} className=${cx('mc', 'p-' + pos(k), c0 && dragging && 'drag', c0 && fly && 'fly')} style=${c0 ? { transform: 'translateX(' + cur + 'px) rotate(' + cur * 0.04 + 'deg)' } : undefined}
        onPointerDown=${c0 ? down : undefined} onPointerMove=${c0 ? move : undefined} onPointerUp=${c0 ? up : undefined} onPointerCancel=${c0 ? up : undefined}>
        <${Av} p=${p} cls="mc-ph" /><span className="mc-shade"></span>
        <div className="mc-who"><${Av} p=${p} cls="mc-mini" /><div><b>@${p.username}</b><small>${p.country_name || 'Somewhere on Earth'}</small></div></div>
        ${p.score != null && html`<span className="mc-pct">${p.score}% match</span>`}
        ${c0 && html`<span className="mc-stamp link" style=${{ opacity: Math.max(0, Math.min(1, dx / 65)) || (fly === 'link' ? 1 : 0) }}><${CIcon} n="link" size=${42} sw=${2.2} /></span><span className="mc-stamp skip" style=${{ opacity: Math.max(0, Math.min(1, -dx / 65)) || (fly === 'skip' ? 1 : 0) }}><${CIcon} n="x" size=${42} sw=${2.2} /></span>`}
        <div className="mc-info"><b>${p.display_name}</b>
          <div className="mc-meta"><span>${p.step || 'Step 1'}</span><i></i><span>${p.study_year || 'Level not set'}</span><i></i><span><${CIcon} n="pin" size=${14} sw=${2} />${p.country_name || 'Earth'}</span></div>
          <p>${p.bio || 'No note yet.'}</p>
          <div className="mc-pills"><button className="mc-pill ico" onClick=${() => act('skip')} aria-label="Skip"><${CIcon} n="x" size=${20} /></button>
            <span className="mc-pill stat">${[hrs(p.weekly_minutes), p.accuracy ? p.accuracy + '% scores' : ''].filter(Boolean).join(' · ') || 'New here'}</span>
            <button className="mc-pill go" onClick=${() => act('link')} aria-label="Send a connect request"><${CIcon} n="link" size=${20} sw=${2.2} /></button></div></div></div>`; })}</div>
    <div className="mc-strip" role="tablist" aria-label="All matches">${items.map((p, k) => html`<button key=${p.user_id} className=${cx(k === i && 'on', k < i && 'seen')} onClick=${() => { setDx(0); setI(k); }} aria-label=${p.display_name}><${Av} p=${p} cls="mc-sa" /></button>`)}</div>
    <p className="mc-hint">Swipe right to link up, left to skip</p></div>`;
}


/* ---------- friends: the cinema-style home, the detail screen, the messages list ---------- */
const phBg = (p) => (p.avatar_url ? 'url("' + p.avatar_url + '")' : 'linear-gradient(145deg, hsl(' + hueOf(p.username || p.display_name || 'x') + ' 70% 52%), hsl(' + (hueOf(p.username || p.display_name || 'x') + 40) + ' 70% 28%))');
function PeopleHome({ list, incoming, waiting, accept, decline, onOpen, onChat, goFind, sampleNote }) {
  const [ptab, setPtab] = useState('friends'), [i, setI] = useState(0), [pq, setPq] = useState(''), [unreadOnly, setUnreadOnly] = useState(false);
  const [dx, setDx] = useState(0), [drag, setDrag] = useState(false), [dir, setDir] = useState(0), dr = useRef(null);
  const n = list.length, at = (k) => list[((i + k) % n + n) % n];
  const go = (d) => { if (n < 2) return; setDir(d); setI((v) => ((v + d) % n + n) % n); setDx(0); };
  useEffect(() => { if (ptab !== 'friends') return undefined; const k = (e) => { if (/INPUT|TEXTAREA/.test(e.target.tagName)) return; if (e.key === 'ArrowRight') go(1); else if (e.key === 'ArrowLeft') go(-1); }; window.addEventListener('keydown', k); return () => window.removeEventListener('keydown', k); }, [ptab, n]);
  const down = (e) => { dr.current = { x: e.clientX, moved: false }; setDrag(true); e.currentTarget.setPointerCapture(e.pointerId); };
  const move = (e) => { if (!dr.current) return; const d = e.clientX - dr.current.x; if (Math.abs(d) > 6) dr.current.moved = true; setDx(d); };
  const up = () => { if (!dr.current) return; const { moved } = dr.current; dr.current = null; setDrag(false); if (dx < -60) go(1); else if (dx > 60) go(-1); else { setDx(0); if (!moved) onOpen(at(0)); } };
  const cur = n ? at(0) : null, unreadTotal = list.reduce((s2, x) => s2 + (x.unread || 0), 0), reqN = incoming.length;
  const ql = pq.trim().toLowerCase(), shownList = list.filter((x) => (!unreadOnly || x.unread) && (!ql || (x.display_name + ' ' + x.username + ' ' + (x.last_body || '')).toLowerCase().includes(ql)));
  const pill = (k, label, badge) => html`<button key=${k} className=${cx('fr-pill', ptab === k && 'on')} onClick=${() => setPtab(k)}><span>${label}</span>${badge > 0 && html`<i>${badge}</i>`}</button>`;
  return html`<div className=${cx('fr', 'p-' + ptab)} style=${{ '--ph': cur ? phBg(cur) : 'none' }}>
    <div className="fr-bg" aria-hidden="true"></div>
    <div className="fr-pills">${pill('messages', 'Messages', unreadTotal)}${pill('friends', 'Friends')}${pill('requests', 'Requests', reqN)}</div>
    <div className="fr-sheet">
      ${ptab === 'friends' && html`<${Fragment}>
        <h2 className="fr-title" key=${cur ? cur.conn_id : 'x'}>${cur ? (cur.display_name || '').toUpperCase() : 'NO FRIENDS YET'}</h2>
        ${n === 0 ? html`<div className="cn-gate"><b>No friends yet</b><p>Swipe right on a match to send a request. Friends show up here.</p><button className="cn-btn" onClick=${goFind}>Find students</button></div>` : html`<div className="fr-stage">
          ${n > 2 && html`<div className="fr-layer l2" style=${{ '--ph': phBg(at(2)) }}></div>`}
          ${n > 1 && html`<div className="fr-layer l1" style=${{ '--ph': phBg(at(1)) }}></div>`}
          <div key=${cur.conn_id} className=${cx('fr-card', drag && 'drag', !drag && dir && 'in' + (dir > 0 ? 'r' : 'l'))} style=${{ transform: 'translateX(' + dx + 'px) rotate(' + dx * 0.03 + 'deg)' }} onPointerDown=${down} onPointerMove=${move} onPointerUp=${up} onPointerCancel=${up}>
            <${Av} p=${cur} cls="fr-img" />
            <div className="fr-tags"><span>${cur.step || 'Step 1'}</span>${cur.study_year && html`<span>${cur.study_year}</span>`}</div>
            ${cur.online && html`<span className="fr-live"><i></i>Online</span>`}
          </div></div>
          <div className="fr-dots">${list.slice(0, 9).map((x, k) => html`<i key=${x.conn_id} className=${cx(k === ((i % n) + n) % n && 'on')}></i>`)}</div>
          <div className="fr-act"><button className="fr-ib" onClick=${() => go(-1)} aria-label="Previous"><${CIcon} n="back" size=${20} /></button><button className="fr-msg" onClick=${() => onChat(cur)}><${CIcon} n="chat" size=${20} sw=${2.1} />Message ${cur.display_name}</button><button className="fr-ib" onClick=${() => go(1)} aria-label="Next"><span style=${{ display: 'grid', transform: 'rotate(180deg)' }}><${CIcon} n="back" size=${20} /></span></button></div>
          ${sampleNote && html`<p className="ch-note">These friends are samples until you connect with real students.</p>`}`}
      <//>`}
      ${ptab === 'messages' && html`<div className="ch">
        <div className="ch-hero">
          <label className="ch-search"><${CIcon} n="search" size=${17} sw=${2} /><input value=${pq} onChange=${(e) => setPq(e.target.value)} placeholder="Search..." aria-label="Search chats" /><button type="button" className=${cx('ch-chev', unreadOnly && 'on')} onClick=${() => setUnreadOnly((v) => !v)} aria-label="Show unread only" title="Unread only"><${CIcon} n="chev" size=${20} sw=${2.2} /></button></label>
          <h2>Let’s Stay<br/>Connected</h2>
          <div className="ch-stories"><button className="ch-st add" onClick=${goFind}><span><${CIcon} n="plus" size=${22} sw=${1.6} /></span><small>Add</small></button>${list.slice(0, 12).map((x) => html`<button key=${x.conn_id} className="ch-st" onClick=${() => onChat(x)}><span><${Av} p=${x} cls="ch-sa" /></span><small>${(x.display_name || '').split(' ')[0]}</small></button>`)}</div>
        </div>
        <div className="ch-list">${shownList.length === 0 ? html`<p className="cn-hint">Nothing here yet.</p>` : shownList.map((x) => html`<button key=${x.conn_id} className="ch-row" onClick=${() => onChat(x)}>
          <span className="ch-ava"><${Av} p=${x} cls="ch-av" />${x.online && html`<i className="ch-dot"></i>`}</span>
          <div className="ch-txt"><b>${x.display_name}</b><small className=${cx(x.typing && 'typing')}>${x.typing ? 'typing…' : (x.last_body || 'Say hello')}</small></div>
          <span className="ch-meta"><time>${chatTime(x.last_at)}</time>${x.unread > 0 && html`<i className="ch-badge">${x.unread}</i>`}</span></button>`)}</div>
        ${sampleNote && html`<p className="ch-note">These chats are samples. Real chats appear once you connect with students.</p>`}</div>`}
      ${ptab === 'requests' && html`<div className="fr-req">
        ${incoming.length === 0 && waiting.length === 0 && html`<p className="cn-hint">No requests right now. Swipe right on a match to send one.</p>`}
        ${incoming.length > 0 && html`<${Fragment}><small className="cn-lab">WANT TO CONNECT</small><div className="pp-list">${incoming.map((x) => html`<div key=${x.conn_id} className="pp-row static"><${Av} p=${x} cls="pp-av" /><div className="pp-txt"><b>${x.display_name}</b><small>@${x.username}</small></div><button className="cn-mini" onClick=${() => accept(x)}>Accept</button><button className="cn-mini ghost" onClick=${() => decline(x)}>Not now</button></div>`)}</div><//>`}
        ${waiting.length > 0 && html`<${Fragment}><small className="cn-lab">WAITING</small><div className="pp-list">${waiting.map((x) => html`<div key=${x.conn_id} className="pp-row static"><${Av} p=${x} cls="pp-av" /><div className="pp-txt"><b>${x.display_name}</b><small>Request sent</small></div><button className="cn-mini ghost" onClick=${() => decline(x)}>Cancel</button></div>`)}</div><//>`}</div>`}
    </div></div>`;
}
function FriendDetail({ p, others, onBack, onMessage }) {
  const sc = useRef(null), txt = useRef(null), facts = useRef(null), cast = useRef(null), waved = useRef(false);
  const text = p.about || p.bio || 'No note yet.', words = text.split(/\s+/).filter(Boolean);
  const rows = [['Exam step', p.step || 'Not set'], ['Level', p.study_year || 'Not set'], ['Country', p.country_name || 'Somewhere on Earth'], ['Study streak', p.streak ? p.streak + ' days' : 'Just getting started']];
  useEffect(() => { const k = (e) => { if (e.key === 'Escape') { e.stopPropagation(); onBack(); } }; window.addEventListener('keydown', k, true); return () => window.removeEventListener('keydown', k, true); }, [onBack]);
  useEffect(() => {
    const el = sc.current; if (!el) return undefined; let raf = 0; const cl = (v, a, b) => Math.max(a, Math.min(b, v));
    const upd = () => {
      raf = 0; const vh = el.clientHeight, y = el.scrollTop;
      el.style.setProperty('--p', cl(y / (vh * 0.85), 0, 1).toFixed(3));
      const t = txt.current;
      if (t) { const top = t.offsetTop - y, r = cl((vh * 0.9 - top) / (vh * 0.55), 0, 1), n = t.children.length; for (let i = 0; i < n; i++) t.children[i].style.opacity = (0.12 + 0.88 * cl(r * (n + 5) - i, 0, 1)).toFixed(2); }
      const cs = cast.current; if (cs && !waved.current && cs.getBoundingClientRect().top < vh * 0.82) { waved.current = true; cs.classList.add('wave'); }
      const f = facts.current; if (f) { const r = cl((vh * 0.95 - (f.offsetTop - y)) / (vh * 0.3), 0, 1); f.style.opacity = r.toFixed(2); f.style.transform = 'translateY(' + ((1 - r) * 24).toFixed(1) + 'px)'; }
    };
    const on = () => { if (!raf) raf = requestAnimationFrame(upd); };
    el.addEventListener('scroll', on, { passive: true }); window.addEventListener('resize', on); upd();
    return () => { el.removeEventListener('scroll', on); window.removeEventListener('resize', on); cancelAnimationFrame(raf); };
  }, [p]);
  return html`<div className="fv" ref=${sc} style=${{ '--ph': phBg(p), '--p': 0 }}>
    <div className="fv-stage">
      <div className="fv-bg" aria-hidden="true"></div>
      <div className="fv-photo"><${Av} p=${p} cls="fv-img" /></div>
      <div className="fv-shade" aria-hidden="true"></div>
      <header className="fv-top"><button className="fv-back" onClick=${onBack} aria-label="Back"><${CIcon} n="back" size=${20} sw=${2} /></button>
        <h3 className="fv-name">${(p.display_name || '').toUpperCase()}</h3>
        <div className="fv-right"><span className="fv-tag">${p.step || 'Step 1'}</span>${p.study_year && html`<span className="fv-tag">${p.study_year}</span>`}<button className="fv-msgbtn" onClick=${onMessage}><${CIcon} n="chat" size=${18} sw=${2.1} /><em>Message</em></button></div></header>
      <div className="fv-hint" aria-hidden="true"><i></i>Scroll</div>
    </div>
    <div className="fv-flow">
      <small className="fv-lab">ABOUT</small>
      <h2 className="fv-h2">${(p.display_name || '').split(' ')[0]}</h2>
      <p className="fv-text" ref=${txt}>${words.map((w, i) => html`<span key=${i}>${w} </span>`)}</p>
      <div className="fv-facts" ref=${facts}>${rows.map(([k, v]) => html`<div key=${k}><b>${k}</b><span>${v}</span></div>`)}</div>
      ${others.length > 0 && html`<h4 className="fv-h4">Study circle</h4><div className="fv-cast" ref=${cast}>${others.slice(0, 10).map((x, i) => html`<div key=${x.conn_id || x.user_id} className="fv-c" style=${{ '--i': i }}><${Av} p=${x} cls="fv-ci" /><span>${(x.display_name || '').split(' ')[0]}</span></div>`)}</div>`}
      <button className="fv-cta" onClick=${onMessage}><${CIcon} n="chat" size=${18} sw=${2.1} />Message ${(p.display_name || '').split(' ')[0]}</button>
    </div></div>`;
}

/* ---------- the connect layer ---------- */
function ConnectUI({ close, deepUser }) {
  const c = useCloud(), name = useUserName();
  const [tab, setTab] = useState('matches'), [fd, setFd] = useState(null);
  const [matches, setMatches] = useState(null), [people, setPeople] = useState([]), [results, setResults] = useState([]), [q, setQ] = useState('');
  const [sheet, setSheet] = useState(null), [chat, setChat] = useState(null), [sent, setSent] = useState({});
  const on = !!(c.profile && c.profile.discoverable), hasName = !!(c.profile && c.profile.username);
  const loadMatches = useCallback(async () => { if (!sb || !CloudStore.get().user) return; await publishStats(); const { data, error } = await sb.rpc('find_matches_v2', { max_rows: 18 }); setMatches(error ? [] : data || []); }, []);
  const loadPeople = useCallback(async () => { if (!sb || !CloudStore.get().user) return; const { data } = await sb.rpc('my_connections'); setPeople(data || []); }, []);
  useEffect(() => { if (c.user && on) { loadMatches(); } else setMatches([]); loadPeople(); }, [c.user && c.user.id, on]);
  useEffect(() => { if (!deepUser || !c.user) return; sb.rpc('public_profile_v2', { u: deepUser }).then(({ data }) => { if (data && data[0]) setSheet(data[0]); else toast('That student is not available.'); }); }, [deepUser, c.user && c.user.id]);
  useEffect(() => {
    if (tab !== 'find' || q.trim().length < 2 || !c.user) { setResults([]); return undefined; }
    let live = true; const t = setTimeout(async () => { const { data } = await sb.rpc('search_users_v2', { q: q.trim().replace(/^@/, '') }); if (live) setResults(data || []); }, 300);
    return () => { live = false; clearTimeout(t); };
  }, [q, tab, c.user && c.user.id]);
  const incoming = people.filter((x) => x.status === 'pending' && !x.i_requested), connected = people.filter((x) => x.status === 'accepted');
  const stateOf = (p) => (connected.some((x) => x.other_id === p.user_id) ? 'connected' : sent[p.user_id] || people.some((x) => x.other_id === p.user_id) ? 'sent' : '');
  const connect = async (p) => {
    if (p.sample) { toast('That is a sample student. Real matches appear as people join.'); return; }
    const { error } = await sb.rpc('request_connection', { target: p.user_id });
    if (error) toast(/not available/i.test(error.message) ? 'That student is not available.' : error.message); else { toast('Request sent to ' + p.display_name); setSent((s) => ({ ...s, [p.user_id]: 'sent' })); loadPeople(); }
  };
  const accept = async (x) => { await sb.from('connections').update({ status: 'accepted' }).eq('id', x.conn_id); toast('You are connected with ' + x.display_name); loadPeople(); };
  const decline = async (x) => { await sb.from('connections').delete().eq('id', x.conn_id); loadPeople(); };
  const turnOn = async () => { if (!hasName) { patchCloud({ profileOpen: true }); toast('Choose a username first, then switch matching on.'); return; } await setDiscoverable(true); };
  const real = matches && matches.length ? matches : null;
  const list = real || (on ? SAMPLE_MATCHES : []);
  const titles = { matches: 'Matches', find: 'Find', people: 'People', code: 'My code' };
  const Row = ({ p }) => html`<button className="cn-row" onClick=${() => setSheet(p)}><${Av} p=${p} cls="cn-av" /><div className="cn-row-t"><b>${p.display_name}</b><small>@${p.username}${p.study_year ? ' · ' + p.study_year : ''}${p.country_name ? ' · ' + p.country_name : ''}</small>${p.score != null && html`<div className="cn-chips">${whyChips(p).map((w) => html`<span key=${w}>${w}</span>`)}</div>`}</div>${p.score != null ? html`<span className="cn-pct">${p.score}%</span>` : ''}</button>`;
  return html`<div className="cn-ui"><div className=${cx('cn-wrap', tab === 'people' && c.user && !chat && !fd && !sheet && 'fr-on', fd && 'fd-on')}>
    ${chat ? html`<${Chat} conn=${chat} onBack=${() => setChat(null)} />` : fd ? html`<${FriendDetail} p=${fd} others=${(connected.length ? connected : SAMPLE_FRIENDS).filter((x) => x.conn_id !== fd.conn_id)} onBack=${() => setFd(null)} onMessage=${() => { setChat(fd); setFd(null); }} />` : sheet ? html`<${PeekSheet} p=${sheet} state=${stateOf(sheet)} onBack=${() => setSheet(null)} onConnect=${connect} />` : html`<${Fragment}>
      <div className="cn-top"><button className="cn-back" onClick=${close} aria-label="Close"><${CIcon} n="back" size=${22} /></button><small className="cn-lab">${tab === 'matches' ? 'TODAY' : 'CONNECT'}</small></div>
      ${tab !== 'people' && html`<h1 className="cn-h">${titles[tab]}</h1>`}
      ${tab === 'matches' && html`<${Fragment}>
        ${!c.user && html`<div className="cn-gate"><b>Matching works both ways</b><p>Sign in and switch matching on, and you will see students who study like you. Without it, you only see the empty planet.</p><button className="cn-btn" onClick=${() => { patchCloud({ modal: true }); }}>Sign in</button></div>`}
        ${c.user && !on && html`<div className="cn-gate"><b>Turn on matching to see others</b><p>It is a two-way street: matching shows you other students only while you are visible to them too. That shares your username, photo, bio, exam step, weekly study time, score range, streak, exam month and goals. Never your notes or answers. Switch it off any time.</p><button className="cn-btn" onClick=${turnOn}>${hasName ? 'Turn on matching' : 'Choose a username first'}</button></div>`}
        ${c.user && on && matches === null && html`<p className="cn-hint">Finding your matches…</p>`}
        ${c.user && on && matches !== null && html`<${Fragment}>${!real && html`<p className="cn-hint">No real matches yet. As more students switch matching on, your best fits appear here. These cards are samples.</p>`}<${MatchDeck} items=${list} onLink=${connect} onOpen=${setSheet} /><//>`}<//>`}
      ${tab === 'find' && html`<${Fragment}><label className="cn-search"><${CIcon} n="search" size=${18} /><input value=${q} onChange=${(e) => setQ(e.target.value)} placeholder="Search a username" aria-label="Search a username" autoCapitalize="none" /></label>
        ${!c.user && html`<p className="cn-hint">Sign in to search students by username.</p>`}
        ${q.trim().length >= 2 && results.length === 0 && c.user && html`<p className="cn-hint">No student with that username is available.</p>`}
        <div className="cn-list">${results.map((p) => html`<${Row} key=${p.user_id} p=${p} />`)}</div><//>`}
      ${tab === 'people' && html`<${Fragment}>
        ${!c.user && html`<div className="cn-gate"><b>Your people live here</b><p>Sign in to see your friends, chat, and keep up with how they are studying.</p><button className="cn-btn" onClick=${() => patchCloud({ modal: true })}>Sign in</button></div>`}
        ${c.user && html`<${PeopleHome} list=${connected.length ? connected : SAMPLE_FRIENDS} sampleNote=${!connected.length} incoming=${incoming} waiting=${people.filter((x) => x.status === 'pending' && x.i_requested)} accept=${accept} decline=${decline} onOpen=${setFd} onChat=${setChat} goFind=${() => setTab('find')} />`}<//>`}
      ${tab === 'code' && html`<${QrCard} username=${c.profile && c.profile.username} />`}
      <nav className="cn-tabs" aria-label="Connect">${[['matches', 'compass'], ['find', 'search'], ['people', 'people'], ['code', 'qr']].map(([k, ic]) => html`<button key=${k} className=${cx(tab === k && 'on')} onClick=${() => setTab(k)} aria-label=${titles[k]} title=${titles[k]}><${CIcon} n=${ic} size=${24} />${k === 'people' && incoming.length > 0 && html`<i>${incoming.length}</i>`}</button>`)}</nav><//>`}
  </div></div>`;
}
function ConnectLayer({ deepUser }) {
  const [stage, setStage] = useState('closed'), [o, setO] = useState({ x: 56, y: 40 }), btn = useRef(null);
  const open = (from) => { const r = from && btn.current ? btn.current.getBoundingClientRect() : null; setO(r ? { x: r.left + r.width / 2, y: r.top + r.height / 2 } : { x: 56, y: 40 }); setStage('opening'); requestAnimationFrame(() => requestAnimationFrame(() => setStage('open'))); };
  const close = () => { setStage('closing'); setTimeout(() => setStage('closed'), 420); };
  useEffect(() => { if (deepUser && stage === 'closed') open(false); }, [deepUser]);
  useEffect(() => { if (stage === 'closed') return undefined; document.body.style.overflow = 'hidden'; const k = (e) => { if (e.key === 'Escape') close(); }; window.addEventListener('keydown', k); return () => { document.body.style.overflow = ''; window.removeEventListener('keydown', k); }; }, [stage]);
  return html`<${Fragment}>
    <button ref=${btn} className="cn-plus" onClick=${() => open(true)} aria-label="Connect with students"><svg viewBox="0 0 24 24" width="26" height="26" fill="none" stroke="#fff" strokeWidth="2.8" strokeLinecap="round"><path d="M12 5v14M5 12h14"/></svg></button>
    ${stage !== 'closed' && ReactDOM.createPortal(html`<div className=${cx('cn-root', stage)} style=${{ '--ox': o.x + 'px', '--oy': o.y + 'px' }}><i className="cn-ring r1"></i><i className="cn-ring r2"></i><i className="cn-ring r3"></i>${(stage === 'open' || stage === 'closing') && html`<${ConnectUI} close=${close} deepUser=${deepUser} />`}</div>`, document.body)}
  <//>`;
}

/* ---------- the Study page ---------- */
function Study({ go, openCase }) {
  const [tab, setTab] = usePref('studyTab', 'notes');
  const due = useStore(CardStore).filter((c) => c.due <= Date.now()).length;
  return html`<section className="sec lib study" data-tone="paper" data-nav="light" style=${{ minHeight: '100dvh' }}>
    <div className="wrap">
      <p className="eyebrow">Your study desk</p>
      <h1 className="st-h">Think it. <span className="hl">Draw it. Keep it.</span></h1>
      <div className="st-tabs"><${Seg} value=${tab} onChange=${setTab} options=${[['notes', 'Notes'], ['cards', `Cards${due ? ' · ' + due : ''}`], ['mistakes', 'Mistakes'], ['plan', 'Planner'], ['focus', 'Focus']]} /></div>
      ${tab === 'notes' && html`<${NotesTab} />`}
      ${tab === 'cards' && html`<${CardsTab} />`}
      ${tab === 'mistakes' && html`<${MistakesTab} go=${go} openCase=${openCase} />`}
      ${tab === 'plan' && html`<${PlannerTab} />`}
      ${tab === 'focus' && html`<${FocusTab} />`}
    </div>
  </section>`;
}


/* ==========================================================================
   CLOUD: Supabase accounts + sync. Everything keeps working offline and on
   this device; signing in just keeps your data in step across devices.
   ========================================================================== */
const SB_URL = 'https://womakbkesvwzwfviwlgs.supabase.co';
const SB_KEY = 'sb_publishable_lAY6AW3Ho6ZlWy83ox3FgQ_Znh_mj-G';     /* publishable key: safe in the browser, row-level security protects the data */
const sb = window.supabase ? window.supabase.createClient(SB_URL, SB_KEY, { auth: { persistSession: true, autoRefreshToken: true, experimental: { passkey: true } } }) : null;
const memStore = (init) => { let v = init; const subs = new Set(); return { get: () => v, set(n) { v = n; subs.forEach((f) => f()); }, subscribe(fn) { subs.add(fn); return () => subs.delete(fn); } }; };
const CloudStore = memStore({ user: null, profile: null, status: 'out', modal: false, mode: 'in', ready: !sb, profileOpen: false, error: '' });
const patchCloud = (p) => CloudStore.set({ ...CloudStore.get(), ...p });
const useCloud = () => useSyncExternalStore(CloudStore.subscribe, CloudStore.get);

const unionBy = (idf, prefer) => (a, b) => { const m = new Map(); [...a, ...b].forEach((x) => { const k = idf(x), o = m.get(k); m.set(k, o ? (prefer ? prefer(o, x) : o) : x); }); return [...m.values()]; };
const SYNC = {
  attempts: { st: window.AttemptStore, merge: (l, r) => unionBy((a) => a.timestamp)(l, r).sort((a, b) => a.timestamp - b.timestamp).slice(-200) },
  saved: { st: SavedStore, merge: unionBy((x) => x.url), pack: (l) => l.map((p) => ({ ...p, extract: (p.extract || '').slice(0, 200) })) },   /* the full text is fetched again when you open the page */
  annotations: { st: AnnStore, merge: (l, r) => {
    const out = { ...r };
    Object.entries(l).forEach(([u, a]) => { const b = out[u]; out[u] = !b ? a : { ...b, ...a, hl: unionBy((h) => (typeof h === 'string' ? h : h.t))(hlList(a), hlList(b)), note: a.note || b.note }; });
    return out; } },
  notes: { st: NotesStore, merge: unionBy((n) => n.id, (o, x) => (x.updated > o.updated ? x : o)) },
  cards: { st: CardStore, merge: unionBy((c) => c.id, (o, x) => (x.reps > o.reps ? x : o)) },
  focus: { st: FocusLog, merge: (l, r) => unionBy((f) => f.at)(l, r).sort((a, b) => a.at - b.at).slice(-500) },
  mastered: { st: MasteredStore, merge: (l, r) => [...new Set([...l, ...r])] },
  goals: { st: GoalsStore, lww: true },
  prefs: { st: PrefsStore, lww: true },
  plan: { st: PlanStore, lww: true },
  name: { st: UserStore, lww: true },
};
const TS_KEY = 'usmle-site:sync-ts';
const tsAll = () => { try { return JSON.parse(localStorage.getItem(TS_KEY) || '{}'); } catch { return {}; } };
const tsMark = (k) => { try { localStorage.setItem(TS_KEY, JSON.stringify({ ...tsAll(), [k]: Date.now() })); } catch { /* ignore */ } };
const isBlank = (v) => v == null || v === '' || (Array.isArray(v) && !v.length) || (typeof v === 'object' && !Array.isArray(v) && !Object.keys(v).length);
let applying = false, warned = false, lastPull = 0; const timers = {}, lastPushed = {}, pending = new Set();

async function pullAndMerge(userId) {
  patchCloud({ status: 'syncing', error: '' });
  const { data, error } = await sb.from('user_state').select('key,value,updated_at').eq('user_id', userId);
  if (error) throw error;
  const remote = Object.fromEntries(data.map((r) => [r.key, r]));
  const rows = [];
  applying = true;
  try {
    for (const [k, def] of Object.entries(SYNC)) {
      const loc = def.st.get(), r = remote[k], touched = !!tsAll()[k];
      let next;
      if (!r) next = loc;
      else if (def.lww) next = (tsAll()[k] || 0) > Date.parse(r.updated_at) ? loc : r.value;
      else next = def.merge(loc, r.value);
      if (JSON.stringify(next) !== JSON.stringify(loc)) def.st.set(next);
      const out = def.pack ? def.pack(next) : next;
      const same = r && JSON.stringify(out) === JSON.stringify(r.value);
      if (same) lastPushed[k] = JSON.stringify(out);
      if (!same && !isBlank(out) && (!def.lww || touched || r)) rows.push({ user_id: userId, key: k, value: out, updated_at: new Date().toISOString() });
    }
  } finally { applying = false; }
  rows.forEach((r) => { lastPushed[r.key] = JSON.stringify(r.value); });
  if (rows.length) { const { error: e2 } = await sb.from('user_state').upsert(rows); if (e2) throw e2; }
  lastPull = Date.now(); patchCloud({ status: 'synced' });
}
async function pushKey(k) {
  const u = CloudStore.get().user; if (!u || !sb) return;
  pending.delete(k); clearTimeout(timers[k]);
  const value = SYNC[k].pack ? SYNC[k].pack(SYNC[k].st.get()) : SYNC[k].st.get(), sig = JSON.stringify(value);
  if (lastPushed[k] === sig) return;                                                                          /* identical to what the cloud already has: no write */
  patchCloud({ status: 'syncing' });
  const { error } = await sb.from('user_state').upsert({ user_id: u.id, key: k, value, updated_at: new Date().toISOString() });
  if (!error) lastPushed[k] = sig;
  if (error) { patchCloud({ status: 'error', error: error.message }); if (!warned) { warned = true; toast('Could not sync just now. Your data is safe on this device.'); } } else { patchCloud({ status: 'synced' }); if (['attempts', 'focus', 'plan', 'goals'].includes(k)) publishStats().catch(() => {}); }
}
function startCloud() {
  if (startCloud.done) return; startCloud.done = true;
  Object.entries(SYNC).forEach(([k, def]) => def.st.subscribe(() => { if (applying) return; tsMark(k); if (CloudStore.get().user) { pending.add(k); clearTimeout(timers[k]); timers[k] = setTimeout(() => pushKey(k), 20000); } }));
  const flush = () => { [...pending].forEach((k) => pushKey(k)); };
  document.addEventListener('visibilitychange', () => { if (document.visibilityState === 'hidden') flush(); }); window.addEventListener('pagehide', flush);
  if (!sb) return;
  const hydrate = async (session) => {
    const user = session && session.user; patchCloud({ user: user || null, ready: true });
    if (!user) { patchCloud({ profile: null, status: 'out' }); return; }
    try {
      await pullAndMerge(user.id);
      const { data: prof } = await sb.from('profiles').select('display_name,is_admin,username,bio,study_year,exam_date,avatar_url,discoverable,step').eq('id', user.id).maybeSingle();
      patchCloud({ profile: prof || null });
      if (prof && prof.display_name && !tsAll().name && UserStore.get() === 'Charles') { applying = true; UserStore.set(prof.display_name); applying = false; }
      { let ls = 0; try { ls = Number(localStorage.getItem('usmle-site:seen') || 0); } catch { /* ignore */ } if (Date.now() - ls > 1800000) { try { localStorage.setItem('usmle-site:seen', String(Date.now())); } catch { /* ignore */ } sb.from('profiles').update({ last_seen: new Date().toISOString() }).eq('id', user.id).then(() => {}); } }
      autoPlace(user); publishStats().catch(() => {});
    } catch (e) { patchCloud({ status: 'error', error: String(e.message || e) }); }
  };
  let hydratedUid = null;
  sb.auth.onAuthStateChange((ev, session) => { const uid = session && session.user ? session.user.id : null; if (ev === 'TOKEN_REFRESHED' || (uid && uid === hydratedUid)) return; hydratedUid = uid; setTimeout(() => hydrate(session), 0); });   /* only when the person actually changes, not on every focus or token refresh */
  document.addEventListener('visibilitychange', () => { const u = CloudStore.get().user; if (document.visibilityState === 'visible' && u && CloudStore.get().status !== 'syncing' && Date.now() - lastPull > 1800000) pullAndMerge(u.id).catch(() => {}); });
}
const Cloud = {
  async signUp(email, password, name) { const { data, error } = await sb.auth.signUp({ email, password, options: { data: { name } } }); if (error) throw error; return data; },
  async signIn(email, password) { const { error } = await sb.auth.signInWithPassword({ email, password }); if (error) throw error; },
  async github() {
    if (!/^https?:$/.test(location.protocol)) throw new Error('FILE_ORIGIN');
    const { error } = await sb.auth.signInWithOAuth({ provider: 'github', options: { redirectTo: location.href.split('#')[0] } }); if (error) throw error;
  },
  async passkey() {
    if (!/^https?:$/.test(location.protocol)) throw new Error('FILE_ORIGIN');
    const { error } = await sb.auth.signInWithPasskey(); if (error) throw error;
  },
  async reset(email) { const { error } = await sb.auth.resetPasswordForEmail(email, { redirectTo: location.href.split('#')[0] }); if (error) throw error; },
  async out(wipe) { await sb.auth.signOut(); if (wipe) { Object.keys(localStorage).filter((k) => /^usmle-(site|quiz):/.test(k)).forEach((k) => localStorage.removeItem(k)); location.reload(); } },
};

const A2N = {"AW":"533","AF":"004","AO":"024","AI":"660","AX":"248","AL":"008","AD":"020","AE":"784","AR":"032","AM":"051","AS":"016","AQ":"010","TF":"260","AG":"028","AU":"036","AT":"040","AZ":"031","BI":"108","BE":"056","BJ":"204","BF":"854","BD":"050","BG":"100","BH":"048","BS":"044","BA":"070","BL":"652","SH":"654","BY":"112","BZ":"084","BM":"060","BO":"068","BQ":"535","BR":"076","BB":"052","BN":"096","BT":"064","BV":"074","BW":"072","CF":"140","CA":"124","CC":"166","CH":"756","CL":"152","CN":"156","CI":"384","CM":"120","CD":"180","CG":"178","CK":"184","CO":"170","KM":"174","CV":"132","CR":"188","CU":"192","CW":"531","CX":"162","KY":"136","CY":"196","CZ":"203","DE":"276","DJ":"262","DM":"212","DK":"208","DO":"214","DZ":"012","EC":"218","EG":"818","ER":"232","EH":"732","ES":"724","EE":"233","ET":"231","FI":"246","FJ":"242","FK":"238","FR":"250","FO":"234","FM":"583","GA":"266","GB":"826","GE":"268","GG":"831","GH":"288","GI":"292","GN":"324","GP":"312","GM":"270","GW":"624","GQ":"226","GR":"300","GD":"308","GL":"304","GT":"320","GF":"254","GU":"316","GY":"328","HK":"344","HM":"334","HN":"340","HR":"191","HT":"332","HU":"348","ID":"360","IM":"833","IN":"356","IO":"086","IE":"372","IR":"364","IQ":"368","IS":"352","IL":"376","IT":"380","JM":"388","JE":"832","JO":"400","JP":"392","KZ":"398","KE":"404","KG":"417","KH":"116","KI":"296","KN":"659","KR":"410","KW":"414","LA":"418","LB":"422","LR":"430","LY":"434","LC":"662","LI":"438","LK":"144","LS":"426","LT":"440","LU":"442","LV":"428","MO":"446","MF":"663","MA":"504","MC":"492","MD":"498","MG":"450","MV":"462","MX":"484","MH":"584","MK":"807","ML":"466","MT":"470","MM":"104","ME":"499","MN":"496","MP":"580","MZ":"508","MR":"478","MS":"500","MQ":"474","MU":"480","MW":"454","MY":"458","YT":"175","NA":"516","NC":"540","NE":"562","NF":"574","NG":"566","NI":"558","NU":"570","NL":"528","NO":"578","NP":"524","NR":"520","NZ":"554","OM":"512","PK":"586","PA":"591","PN":"612","PE":"604","PH":"608","PW":"585","PG":"598","PL":"616","PR":"630","KP":"408","PT":"620","PY":"600","PS":"275","PF":"258","QA":"634","RE":"638","RO":"642","RU":"643","RW":"646","SA":"682","SD":"729","SN":"686","SG":"702","GS":"239","SJ":"744","SB":"090","SL":"694","SV":"222","SM":"674","SO":"706","PM":"666","RS":"688","SS":"728","ST":"678","SR":"740","SK":"703","SI":"705","SE":"752","SZ":"748","SX":"534","SC":"690","SY":"760","TC":"796","TD":"148","TG":"768","TH":"764","TJ":"762","TK":"772","TM":"795","TL":"626","TO":"776","TT":"780","TN":"788","TR":"792","TV":"798","TW":"158","TZ":"834","UG":"800","UA":"804","UM":"581","UY":"858","US":"840","UZ":"860","VA":"336","VC":"670","VE":"862","VG":"092","VI":"850","VN":"704","VU":"548","WF":"876","WS":"882","YE":"887","ZA":"710","ZM":"894","ZW":"716"};

/* Works out a country, a state and a rounded area (whole degrees, about 100 km) from the connection.
   A free lookup service is asked once; nothing precise is kept, and the address is never stored. */
async function detectPlace() {
  const tries = ['https://ipwho.is/', 'https://ipapi.co/json/'];
  for (const u of tries) {
    try {
      const r = await fetch(u); if (!r.ok) continue; const j = await r.json();
      const cc = j.country_code, id = A2N[cc]; if (!id || j.success === false) continue;
      return { id, country: j.country || j.country_name || cc, region: (j.region || '').slice(0, 80) || null, lat: Math.round(j.latitude), lng: Math.round(j.longitude) };
    } catch { /* try the next one */ }
  }
  throw new Error('Could not work out your location');
}
async function placeMe(user, quiet) {
  const g = await detectPlace();
  const full = { user_id: user.id, country_id: g.id, country_name: g.country, region: g.region, lat: g.lat, lng: g.lng };
  let { error } = await sb.from('presence').upsert(full);
  if (error && /column|schema cache/i.test(error.message || '')) ({ error } = await sb.from('presence').upsert({ user_id: user.id, country_id: g.id, country_name: g.country }));
  if (error) throw error;
  if (!quiet) toast('You are counted on the community globe as ' + (g.region ? g.region + ', ' : '') + g.country + '.');
  return g;
}
async function autoPlace(user) {
  try {
    if (!PrefsStore.get().globe) return;
    const { data, error } = await sb.from('presence').select('user_id').eq('user_id', user.id).maybeSingle();
    if (error || data) return;
    await placeMe(user, false);
  } catch { /* quiet: the globe is a bonus */ }
}
async function leaveGlobe(user) { await sb.from('presence').delete().eq('user_id', user.id); }

function AuthModal() {
  const c = useCloud();
  const [mode, setMode] = useState('in');
  const [email, setEmail] = useState(''), [pw, setPw] = useState(''), [nm, setNm] = useState(UserStore.get());
  const [busy, setBusy] = useState(false), [err, setErr] = useState(''), [note, setNote] = useState('');
  const [geoOk, setGeoOk] = useState(PrefsStore.get().globe !== false);
  useEffect(() => { if (c.user && c.modal) patchCloud({ modal: false }); }, [c.user, c.modal]);
  useEffect(() => { if (c.modal) { setMode(c.mode || 'in'); setErr(''); setNote(''); } }, [c.modal]);
  useEffect(() => {
    if (!c.modal) return undefined;
    const k = (e) => { if (e.key === 'Escape') patchCloud({ modal: false }); };
    window.addEventListener('keydown', k); return () => window.removeEventListener('keydown', k);
  }, [c.modal]);
  if (!c.modal) return null;
  const friendly = (m) => (m === 'FILE_ORIGIN' ? 'GitHub sign-in and passkeys need the site to be hosted at a web address. They cannot start from a file on your computer. Use email for now.' : /passkey_disabled/i.test(m) ? 'Passkeys are not switched on for this site yet.' : /webauthn_credential_not_found/i.test(m) ? 'No passkey was found for this site on that device.' : /NotAllowedError|cancel|abort/i.test(m) ? 'The passkey prompt was closed.' : /Invalid login/i.test(m) ? 'That email and password do not match.' : /already registered/i.test(m) ? 'There is already an account with that email. Try signing in.' : /provider is not enabled|Unsupported provider/i.test(m) ? 'GitHub sign-in is not switched on yet. Use email for now.' : /Password should/i.test(m) ? 'Please use a password with at least 6 characters.' : /rate limit/i.test(m) ? 'Too many tries. Please wait a minute.' : m);
  const submit = async (e) => {
    e.preventDefault(); setErr(''); setNote(''); setBusy(true);
    try {
      if (mode === 'up') { const d = await Cloud.signUp(email.trim(), pw, nm.trim() || 'friend'); if (!d.session) setNote('Almost there. Check your email and tap the link to confirm, then come back and sign in.'); else toast('Welcome. Your account is ready.'); }
      else if (mode === 'reset') { await Cloud.reset(email.trim()); setNote('If that email has an account, a reset link is on its way.'); }
      else await Cloud.signIn(email.trim(), pw);
    } catch (x) { setErr(friendly(String(x.message || x))); } finally { setBusy(false); }
  };
  return ReactDOM.createPortal(html`<div className="au-back" onMouseDown=${(e) => { if (e.target === e.currentTarget) patchCloud({ modal: false }); }}>
    <form className="au-card" onSubmit=${submit} role="dialog" aria-modal="true" aria-label="Account">
      <button type="button" className="au-x" onClick=${() => patchCloud({ modal: false })} aria-label="Close"><${Icon} n="fa-xmark" /></button>
      <h3>${mode === 'up' ? 'Create your account' : mode === 'reset' ? 'Reset your password' : 'Welcome back'}</h3>
      <p>${mode === 'up' ? 'Free, with no catch. Your notes, cards and scores will follow you to any device.' : mode === 'reset' ? 'We will email you a link.' : 'Sign in to pick up where you left off.'}</p>
      ${mode === 'in' && html`<button type="button" className="au-gh" onClick=${async () => { setErr(''); try { await Cloud.passkey(); } catch (x) { setErr(friendly(String(x.message || x))); } }}><${Icon} n="fa-fingerprint" /> Sign in with a passkey</button>`}
      ${mode !== 'reset' && html`<button type="button" className="au-gh" onClick=${async () => { setErr(''); try { await Cloud.github(); } catch (x) { setErr(friendly(String(x.message || x))); } }}><i className="fab fa-github" aria-hidden="true"></i> Continue with GitHub</button><div className="au-or"><span>or</span></div>`}
      ${mode === 'up' && html`<label>Your name<input value=${nm} onChange=${(e) => setNm(e.target.value)} maxLength="24" autoComplete="name" /></label>`}
      <label>Email<input type="email" required value=${email} onChange=${(e) => setEmail(e.target.value)} autoComplete="email" /></label>
      ${mode !== 'reset' && html`<label>Password<input type="password" required minLength="6" value=${pw} onChange=${(e) => setPw(e.target.value)} autoComplete=${mode === 'up' ? 'new-password' : 'current-password'} /></label>`}
      ${mode === 'up' && html`<label className="au-chk"><input type="checkbox" checked=${geoOk} onChange=${(e) => { setGeoOk(e.target.checked); PrefsStore.set({ ...PrefsStore.get(), globe: e.target.checked }); }} /><span>Add my country and state to the community globe. It is worked out from my connection, only the region is kept, never an address, and I can turn it off any time.</span></label>`}
      ${err && html`<p className="au-err" role="alert">${err}</p>`}${note && html`<p className="au-note">${note}</p>`}
      <button className="au-go" type="submit" disabled=${busy}>${busy ? 'One moment…' : mode === 'up' ? 'Create account' : mode === 'reset' ? 'Send link' : 'Sign in'}</button>
      <div className="au-sw">${mode === 'in' ? html`<button type="button" onClick=${() => { setMode('up'); setErr(''); }}>New here? Create an account</button><button type="button" onClick=${() => { setMode('reset'); setErr(''); }}>Forgot password?</button>` : html`<button type="button" onClick=${() => { setMode('in'); setErr(''); setNote(''); }}>I already have an account</button>`}</div>
    </form></div>`, document.body);
}

const STUDY_YEARS = ['MS1', 'MS2', 'MS3', 'MS4', 'Gap year', 'Graduate', 'Other'];
const squareJpeg = (file, px) => new Promise((ok, no) => {
  const url = URL.createObjectURL(file), im = new Image();
  im.onload = () => {
    const k = Math.min(im.width, im.height), c = document.createElement('canvas'); c.width = c.height = px;
    c.getContext('2d').drawImage(im, (im.width - k) / 2, (im.height - k) / 2, k, k, 0, 0, px, px);
    URL.revokeObjectURL(url); c.toBlob((b) => (b ? ok(b) : no(new Error('Could not read that image'))), 'image/jpeg', 0.85);
  };
  im.onerror = () => { URL.revokeObjectURL(url); no(new Error('That file is not an image we can read')); };
  im.src = url;
});
function ProfileModal() {
  const c = useCloud(), name = useUserName();
  const p = c.profile || {};
  const [f, setF] = useState(null);
  const [avail, setAvail] = useState('');            /* '', 'checking', 'ok', 'taken', 'bad' */
  const [busy, setBusy] = useState(false), [err, setErr] = useState('');
  const fileRef = useRef(null);
  useEffect(() => { if (c.profileOpen) { setF({ name: name || p.display_name || '', username: p.username || '', bio: p.bio || '', year: p.study_year || '', step: p.step || '', exam: p.exam_date || '' }); setErr(''); setAvail(''); } }, [c.profileOpen]);
  useEffect(() => {
    if (!f || !c.profileOpen) return undefined;
    const u = f.username.trim().toLowerCase();
    if (!u || u === (p.username || '')) { setAvail(''); return undefined; }
    if (!/^[a-z0-9_]{3,20}$/.test(u)) { setAvail('bad'); return undefined; }
    setAvail('checking'); let live = true;
    const t = setTimeout(async () => { const { data, error } = await sb.rpc('username_available', { u }); if (live) setAvail(error ? '' : data ? 'ok' : 'taken'); }, 400);
    return () => { live = false; clearTimeout(t); };
  }, [f && f.username, c.profileOpen]);
  useEffect(() => {
    if (!c.profileOpen) return undefined;
    const k = (e) => { if (e.key === 'Escape') patchCloud({ profileOpen: false }); };
    window.addEventListener('keydown', k); return () => window.removeEventListener('keydown', k);
  }, [c.profileOpen]);
  if (!c.profileOpen || !f || !c.user) return null;
  const set = (k, v) => setF({ ...f, [k]: v });
  const uid = c.user.id;
  const savePatch = async (patch) => { const { error } = await sb.from('profiles').update(patch).eq('id', uid); if (error) throw error; patchCloud({ profile: { ...CloudStore.get().profile, ...patch } }); };
  const onPhoto = async (e) => {
    const file = e.target.files && e.target.files[0]; e.target.value = ''; if (!file) return;
    setErr(''); setBusy(true);
    try {
      const blob = await squareJpeg(file, 256);
      const { error } = await sb.storage.from('avatars').upload(uid + '/avatar.jpg', blob, { upsert: true, contentType: 'image/jpeg', cacheControl: '3600' });
      if (error) throw error;
      await savePatch({ avatar_url: SB_URL + '/storage/v1/object/public/avatars/' + uid + '/avatar.jpg?v=' + Date.now() });
      toast('Photo updated');
    } catch (x) { setErr(String(x.message || x)); } finally { setBusy(false); }
  };
  const dropPhoto = async () => { setBusy(true); try { await sb.storage.from('avatars').remove([uid + '/avatar.jpg']); await savePatch({ avatar_url: null }); } catch (x) { setErr(String(x.message || x)); } finally { setBusy(false); } };
  const save = async (e) => {
    e.preventDefault(); setErr('');
    const u = f.username.trim().toLowerCase();
    if (u && !/^[a-z0-9_]{3,20}$/.test(u)) { setErr('Usernames use 3 to 20 letters, numbers or underscores.'); return; }
    if (avail === 'taken') { setErr('That username is taken. Try another.'); return; }
    setBusy(true);
    try {
      const nm = f.name.trim().slice(0, 24) || 'friend';
      await savePatch({ display_name: nm, username: u || null, bio: f.bio.trim().slice(0, 160), study_year: f.year || null, step: f.step || null, exam_date: f.exam || null });
      UserStore.set(nm); if (f.exam) PlanStore.set({ ...PlanStore.get(), exam: f.exam });
      toast('Profile saved'); patchCloud({ profileOpen: false });
    } catch (x) { setErr(/duplicate|unique|23505/i.test(String(x.message || x)) ? 'That username is taken. Try another.' : /violates check/i.test(String(x.message || x)) ? 'That username is not allowed. Try another.' : String(x.message || x)); } finally { setBusy(false); }
  };
  const note = { checking: 'Checking…', ok: 'Available', taken: 'Already taken', bad: '3 to 20 letters, numbers or _' }[avail] || '';
  return ReactDOM.createPortal(html`<div className="au-back" onMouseDown=${(e) => { if (e.target === e.currentTarget) patchCloud({ profileOpen: false }); }}>
    <form className="au-card pf-card" onSubmit=${save} role="dialog" aria-modal="true" aria-label="Your profile">
      <button type="button" className="au-x" onClick=${() => patchCloud({ profileOpen: false })} aria-label="Close"><${Icon} n="fa-xmark" /></button>
      <h3>Your profile</h3>
      <div className="pf-photo">
        <button type="button" className="pf-av" onClick=${() => fileRef.current.click()} disabled=${busy} aria-label="Change photo">${p.avatar_url ? html`<img src=${p.avatar_url} alt="" />` : html`<b>${(f.name || '?').charAt(0).toUpperCase()}</b>`}<span><${Icon} n="fa-camera" /></span></button>
        <div><button type="button" className="pf-link" onClick=${() => fileRef.current.click()} disabled=${busy}>${p.avatar_url ? 'Change photo' : 'Add a photo'}</button>${p.avatar_url && html`<button type="button" className="pf-link" onClick=${dropPhoto} disabled=${busy}>Remove</button>`}<small>Shrunk to a small square on your device</small></div>
        <input ref=${fileRef} type="file" accept="image/*" hidden onChange=${onPhoto} />
      </div>
      <label>Display name<input value=${f.name} maxLength="24" onChange=${(e) => set('name', e.target.value)} /></label>
      <label>Username<span className="pf-at"><i>@</i><input value=${f.username} maxLength="20" onChange=${(e) => set('username', e.target.value.toLowerCase().replace(/[^a-z0-9_]/g, ''))} placeholder="yourname" autoCapitalize="none" spellCheck="false" /></span><small className=${'pf-n ' + avail}>${note}</small></label>
      <label>About you<textarea maxLength="160" value=${f.bio} onChange=${(e) => set('bio', e.target.value)} placeholder="A line about you, your goals, your why…"></textarea><small className="pf-n">${f.bio.length}/160</small></label>
      <label>Exam step<${SelectBox} value=${f.step} onChange=${(v) => set('step', v)} placeholder="Not set" label="Exam step" options=${[['', 'Not set'], ['Step 1', 'Step 1'], ['Step 2 CK', 'Step 2 CK'], ['Step 2 CS', 'Step 2 CS'], ['Step 3', 'Step 3']]} /></label>
      <div className="pf-row"><label>Level<${SelectBox} value=${f.year} onChange=${(v) => set('year', v)} placeholder="Not set" label="Study year" options=${[['', 'Not set'], ...STUDY_YEARS.map((y) => [y, y])]} /></label>
        <label>Step 1 date<${DatePicker} value=${f.exam} onChange=${(v) => set('exam', v)} placeholder="Pick a date" label="Step 1 date" /></label></div>
      ${err && html`<p className="au-err" role="alert">${err}</p>`}
      <button className="au-go" type="submit" disabled=${busy || avail === 'checking'}>${busy ? 'Saving…' : 'Save profile'}</button>
    </form></div>`, document.body);
}

/* A seal-shaped check, drawn once as a path. Green when everything is saved. */
const BADGE_D = (() => {
  const n = 10, pts = []; for (let i = 0; i < n * 2; i++) { const a = (Math.PI * i) / n - Math.PI / 2, r = i % 2 ? 9.1 : 10.7; pts.push([12 + r * Math.cos(a), 12 + r * Math.sin(a)]); }
  const mid = (a, b) => [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2], f = (v) => v.toFixed(2); let d = '';
  for (let i = 0; i < pts.length; i++) { const p0 = pts[i], p1 = pts[(i + 1) % pts.length], m = mid(p0, p1); if (!i) { const s0 = mid(pts[pts.length - 1], p0); d += 'M' + f(s0[0]) + ',' + f(s0[1]); } d += ' Q' + f(p0[0]) + ',' + f(p0[1]) + ' ' + f(m[0]) + ',' + f(m[1]); }
  return d + 'Z';
})();
function SyncBadge({ status, size = 18 }) {
  const ok = status === 'synced', bad = status === 'error', busy = status === 'syncing';
  const fill = bad ? 'url(#sbRed)' : busy ? 'url(#sbGrey)' : ok ? 'url(#sbGreen)' : 'url(#sbGrey)';
  return html`<svg className=${'sbadge ' + (status || 'out')} viewBox="0 0 24 24" width=${size} height=${size} role="img" aria-label=${ok ? 'Everything is saved' : bad ? 'Could not sync' : busy ? 'Saving' : 'Not signed in'}>
    <defs><linearGradient id="sbGreen" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#3ee58a" /><stop offset="1" stop-color="#0c9d4c" /></linearGradient><linearGradient id="sbRed" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#ff7a6e" /><stop offset="1" stop-color="#d1382b" /></linearGradient><linearGradient id="sbGrey" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#b9b9c0" /><stop offset="1" stop-color="#76767e" /></linearGradient></defs>
    <path d=${BADGE_D} fill=${fill} stroke="#fff" stroke-width="1.1" stroke-linejoin="round" />
    ${ok && html`<path className="sb-tick" d="M7.4 12.4l3.1 3.1 6-6.5" fill="none" stroke="#fff" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round" />`}
    ${bad && html`<path d="M12 7.2v6M12 16.4v.3" fill="none" stroke="#fff" stroke-width="2.3" stroke-linecap="round" />`}
    ${busy && html`<circle className="sb-spin" cx="12" cy="12" r="4.4" fill="none" stroke="#fff" stroke-width="2" stroke-linecap="round" stroke-dasharray="14 14" />`}
  </svg>`;
}

function AccountButton() {
  const c = useCloud(), name = useUserName();
  const [stage, setStage] = useState('closed');   /* closed | open | closing */
  const closeMenu = useCallback(() => { setStage('closing'); setTimeout(() => setStage('closed'), 420); }, []);
  const dropR = useRef(null), neckR = useRef(null), bodyR = useRef(null), rectR = useRef(null);
  /* A real drip, in the real nav glass. The drop and the thread are ordinary .glass elements (the same layers as the nav bar and
     the quiz timer: a tint, plus a backdrop displaced by the glass filter), moved by a small physics sim:
     the drop grows (more weight), the thread through the neck holds it and thins as it stretches, so it sags then runs away;
     the thread snaps and springs back; the drop rings, lands, and splashes into the panel on springs. Closing runs it backwards. */
  useLayoutEffect(() => {
    const drop = dropR.current, neck = neckR.current, bd = bodyR.current;
    if (stage === 'closed' || !drop || !neck || !bd) return undefined;
    const W = 280, H = bd.offsetHeight, R = W, AV = 46, GAP = 48, cx0 = W - AV / 2, cy0 = -GAP - AV / 2, OX = 0, OY = 0;
    const L0 = 34, Lp = 60, GRAV = 3400, KH = 150, DAMP = 6, RD = 26, DT = 1 / 480;
    const opening = stage === 'open', clamp = (v, lo, hi) => Math.max(lo, Math.min(hi, v));
    const px = (n) => n + 'px';
    const sneck = (L) => clamp(1 - (L - L0) / (Lp - L0), 0, 1);
    const spring = (e, tg, w, z) => { const a = -w * w * (e.x - tg) - 2 * z * w * e.v; e.v += a * DT; e.x += e.v * DT; };
    const boxEl = (l, t, r, b, rad) => { const st = drop.style, w = Math.max(2, r - l), h = Math.max(2, b - t); st.left = px(l); st.top = px(t); st.width = px(w); st.height = px(h); st.borderRadius = rad == null ? px(Math.min(30, w / 2, h / 2)) : rad; st.opacity = 1; rectR.current = { l, t, r, b }; };
    const hideNeck = () => { neck.style.opacity = 0; };
    const showNeck = (cy, top, width) => { if (width < 2) { hideNeck(); return; } const st = neck.style; st.left = px(cx0 - width / 2); st.top = px(cy); st.width = px(width); st.height = px(Math.max(2, top - cy)); st.borderRadius = px(width / 2); st.opacity = 1; };
    const content = (l0) => {
      const rc = rectR.current, p = clamp((l0 - rc.l) / (l0 - OX), 0, 1);
      bd.style.opacity = clamp((p - 0.6) / 0.3, 0, 1);
      bd.style.clipPath = 'inset(' + px(Math.max(0, rc.t - OY)) + ' ' + px(Math.max(0, R - rc.r)) + ' ' + px(Math.max(0, OY + H - rc.b)) + ' ' + px(Math.max(0, rc.l - OX)) + ')';
    };
    const settle = () => { boxEl(OX, OY, R, OY + H, '30px'); hideNeck(); bd.style.opacity = 1; bd.style.clipPath = 'none'; };
    let raf = 0, t0 = 0, simT = 0, ph, E, y, v, rd, tbreak;
    if (opening) { ph = 'hang'; y = cy0 + 27; v = 0; rd = 10; tbreak = null; bd.style.opacity = 0; drop.style.opacity = 0; hideNeck(); rectR.current = { l: cx0 - 10, t: y - 10, r: cx0 + 10, b: y + 10 }; }
    else { const rc = rectR.current || { l: OX, t: OY, r: R, b: OY + H }; ph = 'collapse'; E = { l: { x: rc.l, v: 0 }, t: { x: rc.t, v: 0 }, r: { x: rc.r, v: 0 }, b: { x: rc.b, v: 0 } }; hideNeck(); }
    const L_START = { v: cx0 - RD };
    const step = () => {
      if (ph === 'hang') {
        const L = y - cy0, s = tbreak == null ? sneck(L) : 0; rd = 10 + 18 * clamp(simT / 0.2, 0, 1);
        const rest = cy0 + AV / 2 + 0.4 * rd, ge = GRAV * (rd / 22) * (rd / 22);
        v += (ge - KH * s * (y - rest) - DAMP * v) * DT; y += v * DT;
        if (tbreak == null && sneck(y - cy0) <= 0.001) tbreak = simT;
        if (y - rd >= OY) { ph = 'land'; E = { l: { x: cx0 - rd, v: 0 }, t: { x: y - rd, v }, r: { x: cx0 + rd, v: 0 }, b: { x: y + rd, v } }; L_START.v = cx0 - rd; }
      } else if (ph === 'land') {
        spring(E.l, OX, 17, 0.42); spring(E.r, R, 24, 0.8); spring(E.t, OY, 20, 0.75); spring(E.b, OY + H, 15, 0.4);
      } else if (ph === 'collapse') {
        spring(E.l, cx0 - RD, 24, 0.95); spring(E.r, cx0 + RD, 24, 0.95); spring(E.t, OY, 24, 0.95); spring(E.b, OY + 2 * RD, 24, 0.95);
        if (Math.abs(E.l.x - (cx0 - RD)) < 2 && Math.abs(E.b.x - (OY + 2 * RD)) < 2) { ph = 'rise'; y = OY + RD; v = 0; rd = RD; }
      } else if (ph === 'rise') {
        v -= 5200 * DT; y += v * DT; const L = y - cy0; rd = RD * clamp((L - 24) / 14, 0, 1) + 0.0001;
        if (L < 25) ph = 'done';
      }
    };
    const paint = () => {
      if (ph === 'hang') {
        const L = y - cy0, s = tbreak == null ? sneck(L) : 0, tbr = tbreak == null ? 0 : simT - tbreak;
        const sy = tbreak == null ? 1 + 0.3 * (1 - s) * clamp((L - 30) / 8, 0, 1) : 1 + 0.2 * Math.exp(-14 * tbr) * Math.cos(46 * tbr), sx = 1 / Math.sqrt(sy);
        const rx = rd * sx, ry = rd * sy; boxEl(cx0 - rx, y - ry, cx0 + rx, y + ry, '50%'); rectR.current = { l: cx0 - rd, t: y - rd, r: cx0 + rd, b: y + rd }; bd.style.opacity = 0;
        const top = cy0 + AV / 2 - 8;
        if (tbreak == null) showNeck(top, y - ry + 12, 24 * Math.pow(s, 0.9)); else showNeck(top, top + 16 * Math.exp(-20 * tbr) + 2, 10 * Math.exp(-22 * tbr));
      } else if (ph === 'land') { boxEl(E.l.x, E.t.x, E.r.x, E.b.x); hideNeck(); content(L_START.v); }
      else if (ph === 'collapse') { boxEl(E.l.x, E.t.x, E.r.x, E.b.x); bd.style.opacity = clamp(parseFloat(bd.style.opacity || 1) - 0.12, 0, 1); bd.style.clipPath = 'inset(' + px(Math.max(0, E.t.x - OY)) + ' ' + px(Math.max(0, R - E.r.x)) + ' ' + px(Math.max(0, OY + H - E.b.x)) + ' ' + px(Math.max(0, E.l.x - OX)) + ')'; }
      else if (ph === 'rise') { const L = y - cy0, s = sneck(L), ry = rd * (1 + 0.28 * (1 - s)); boxEl(cx0 - rd, y - ry, cx0 + rd, y + ry, '50%'); showNeck(cy0 + AV / 2 - 8, y - ry + 12, 24 * Math.pow(s, 0.9)); bd.style.opacity = 0; }
    };
    const tick = (now) => {
      if (!t0) t0 = now; const target = (now - t0) / 1000;
      while (simT < target && ph !== 'done') {
        simT += DT; step();
        if (ph === 'land' && Math.abs(E.l.x - OX) < 0.3 && Math.abs(E.b.x - OY - H) < 0.3 && Math.abs(E.t.x - OY) < 0.3 && Math.abs(E.r.x - R) < 0.3 && Math.abs(E.l.v) < 4 && Math.abs(E.b.v) < 4 && Math.abs(E.t.v) < 4) { settle(); ph = 'rest'; break; }
      }
      if (ph === 'rest') return;
      if (ph === 'done') { drop.style.opacity = 0; hideNeck(); bd.style.opacity = 0; return; }
      paint(); raf = requestAnimationFrame(tick);
    };
    paint(); raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [stage]);
  useEffect(() => {
    if (stage !== 'open') return undefined;
    const d = (e) => { if (!e.target.closest('.acct-wrap')) closeMenu(); }, k = (e) => { if (e.key === 'Escape') closeMenu(); };
    document.addEventListener('mousedown', d); window.addEventListener('keydown', k);
    return () => { document.removeEventListener('mousedown', d); window.removeEventListener('keydown', k); };
  }, [stage, closeMenu]);
  if (!sb) return null;
  if (!c.user) return html`<button className="acct-in glass" onClick=${() => patchCloud({ modal: true })}><${Icon} n="fa-user" /><span className="lbl">Sign in</span></button>`;
  const label = { synced: 'All saved to your account', syncing: 'Saving…', error: 'Could not sync. Data is safe on this device.', out: '' }[c.status];
  const go = (fn) => () => { closeMenu(); fn(); };
  return html`<div className="acct-wrap"><button className="acct-av glass" onClick=${() => (stage === 'open' ? closeMenu() : stage === 'closed' && setStage('open'))} aria-label="Account" aria-expanded=${stage === 'open'}>${c.profile && c.profile.avatar_url ? html`<img src=${c.profile.avatar_url} alt="" />` : html`<b>${(name || '?').trim().charAt(0).toUpperCase()}</b>`}<span className="sb-corner"><${SyncBadge} status=${c.status} size=${19} /></span></button>
    ${stage !== 'closed' && html`<div className=${cx('acct-menu', stage)}>
      <div className="am-neck glass" ref=${neckR} aria-hidden="true"></div><div className="am-drop glass" ref=${dropR} aria-hidden="true"></div>
      <div className="am-body" ref=${bodyR}><div className="am-top"><b>${name}</b><small>${c.profile && c.profile.username ? '@' + c.profile.username : c.user.email}</small></div>
        <p className=${'am-st ' + c.status}><${SyncBadge} status=${c.status} size=${16} />${label}</p>
        <button onClick=${go(() => { location.hash = '#/account'; })}><${Icon} n="fa-id-badge" /> Your account</button>
        <button onClick=${go(() => { location.hash = '#/community'; })}><${Icon} n="fa-earth-europe" /> Community globe</button>
        <button onClick=${go(() => patchCloud({ profileOpen: true }))}><${Icon} n="fa-user-pen" /> ${c.profile && c.profile.username ? 'Edit profile' : 'Set up your profile'}</button>
        <button onClick=${() => { pullAndMerge(c.user.id).catch(() => {}); }}><${Icon} n="fa-arrows-rotate" /> Sync now</button>
        <button onClick=${go(() => Cloud.out(false))}><${Icon} n="fa-arrow-right-from-bracket" /> Sign out</button>
        <button onClick=${go(() => { if (confirm('Sign out and remove this device’s copy of your data? It stays safe in your account.')) Cloud.out(true); })}><${Icon} n="fa-eraser" /> Sign out and clear this device</button></div></div>`}</div>`;
}


/* ==========================================================================
   APP
   ========================================================================== */
function QuizGate({ ready }) {
  return html`<section className="qg" data-nav="dark"><div className="qg-in">
    <p className="eyebrow">The quiz</p>
    <h1>Sign in to <span className="hl">begin.</span></h1>
    ${ready ? html`<p>The quiz keeps your scores, your streak and your mistakes, so you need an account to take it. It is free, and it takes less than a minute.</p>
      <div className="qg-btns"><button className="lh-btn" onClick=${() => patchCloud({ modal: true, mode: 'in' })}><${Icon} n="fa-user" /> Sign in</button><button className="lh-btn ghost" onClick=${() => patchCloud({ modal: true, mode: 'up' })}>Create an account</button></div>`
      : html`<p>One moment…</p>`}</div></section>`;
}

function App() {
  const [route, go] = useRoute();
  useEffect(() => { startCloud(); }, []);
  const attempts = useAttempts();
  const cloud = useCloud();
  const quizOk = !sb || !!cloud.user;
  const [quizMounted, setQuizMounted] = useState(false);
  const [findOpen, setFindOpen] = useState(false);
  const [pendingCase, setPendingCase] = useState(null);
  const [reader, setReader] = useState(null);
  const progressRef = useRef(null);

  useEffect(() => {
    if (route === 'quiz' && quizOk) setQuizMounted(true);
    document.body.classList.toggle('quiz-locked', route === 'quiz' && quizOk);
    document.body.dataset.route = route;
    window.__quizProtect = PROTECT_QUIZ && route === 'quiz' && quizOk;
  }, [route, quizOk]);
  useEffect(() => { window.scrollTo(0, 0); }, [route]);
  useEffect(() => {   /* warm the quiz images while the page is idle, so cards never wait on a picture */
    if (!quizOk) return undefined; const run = () => { try { (window.__quizData ? window.__quizData.questions : []).forEach((q) => { if (q.image) { const im = new Image(); im.decoding = 'async'; im.src = q.image; } }); } catch { /* ignore */ } };
    const id = window.requestIdleCallback ? window.requestIdleCallback(run, { timeout: 2500 }) : setTimeout(run, 1200); return () => { if (window.cancelIdleCallback && window.requestIdleCallback) window.cancelIdleCallback(id); else clearTimeout(id); };
  }, [quizOk]);
  useEffect(() => { if (!quizOk) setQuizMounted(false); }, [quizOk]);

  /* scroll progress bar */
  useEffect(() => {
    let raf = 0;
    const update = () => {
      raf = 0;
      const max = document.documentElement.scrollHeight - window.innerHeight;
      if (progressRef.current) progressRef.current.style.setProperty('--p', max > 0 ? Math.min(1, window.scrollY / max) : 0);
      /* each nav piece looks at what is actually underneath it and picks a light or dark tint */
      ['.brand', '.find', '.tabs', '.stack-btn', '.acct-av', '.acct-in'].forEach((sel) => {
        const el = document.querySelector(sel); if (!el) return;
        const r = el.getBoundingClientRect();
        if (route === 'quiz') { el.dataset.t = 'dark'; return; }
        const cy = r.top + r.height / 2;
        const tones = [0.12, 0.5, 0.88].map((f) => toneAt(r.left + r.width * f, cy));
        el.dataset.t = tones.every((t) => t === tones[0]) ? tones[0] : 'mixed';   /* straddling light and dark */
      });
    };
    const on = () => { if (!raf) raf = requestAnimationFrame(update); };
    window.addEventListener('scroll', on, { passive: true });
    window.addEventListener('resize', on);
    const t = setInterval(update, 600);
    update();
    return () => { window.removeEventListener('scroll', on); window.removeEventListener('resize', on); clearInterval(t); cancelAnimationFrame(raf); };
  }, [route]);

  /* reveal-on-scroll + cursor spotlight */
  useEffect(() => {
    const io = new IntersectionObserver((es) => es.forEach(e => { if (e.isIntersecting) { e.target.classList.add('in'); io.unobserve(e.target); } }), { threshold: 0.12, rootMargin: '0px 0px -6% 0px' });
    const scan = () => document.querySelectorAll('.reveal:not(.in)').forEach(el => io.observe(el));
    scan();
    const mo = new MutationObserver(scan);
    mo.observe(document.getElementById('site'), { childList: true, subtree: true });
    const spot = (e) => { const el = e.target.closest && e.target.closest('.spot'); if (!el) return; const r = el.getBoundingClientRect(); el.style.setProperty('--mx', (e.clientX - r.left) + 'px'); el.style.setProperty('--my', (e.clientY - r.top) + 'px'); };
    document.addEventListener('pointermove', spot, { passive: true });
    return () => { io.disconnect(); mo.disconnect(); document.removeEventListener('pointermove', spot); };
  }, []);

  /* find shortcut */
  const openFind = useCallback(() => setFindOpen(true), []);
  useEffect(() => {
    const k = (e) => {
      const typing = e.target.closest && e.target.closest('input, textarea, [contenteditable]');
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); setFindOpen(o => !o); }
      else if (e.key === '/' && !typing && !e.metaKey && !e.ctrlKey) { e.preventDefault(); setFindOpen(true); }
    };
    window.addEventListener('keydown', k);
    return () => window.removeEventListener('keydown', k);
  }, []);

  const openCase = useCallback((i) => setPendingCase(i), []);
  const onRead = useCallback((c) => setReader(c), []);

  return html`<${Fragment}>
    <${SiteNav} route=${route} gate=${route === 'quiz' && !quizOk} go=${go} openFind=${openFind} />
    <${AuthModal} /><${ProfileModal} />
    <${Tabs} route=${route} gate=${route === 'quiz' && !quizOk} go=${go} />
    <main id="site">
      ${route === 'quiz' && !quizOk && html`<div className="site-view"><${QuizGate} ready=${cloud.ready} /></div>`}
      ${route === 'home' && html`<div className="site-view">
        <${Hero} go=${go} /><${Manifesto} /><${Inside} go=${go} /><${Stack} /><${Orbit} go=${go} /><${Stats} attempts=${attempts} /><${CtaBig} go=${go} /><${QuoteBlock} /><${Footer} go=${go} openFind=${openFind} /></div>`}
      ${route === 'dashboard' && html`<div className="site-view"><${BarsIntro} /><${Dashboard} go=${go} openFind=${openFind} /><${Footer} go=${go} openFind=${openFind} /></div>`}
      ${route === 'community' && html`<div className="site-view"><${Community} go=${go} /><${Footer} go=${go} openFind=${openFind} /></div>`}
      ${route === 'account' && html`<div className="site-view"><${AccountPage} go=${go} openFind=${openFind} /><${Footer} go=${go} openFind=${openFind} /></div>`}
      ${route === 'study' && html`<div className="site-view"><${Study} go=${go} openCase=${openCase} /><${Footer} go=${go} openFind=${openFind} /></div>`}
      ${route === 'library' && html`<div className="site-view"><${LibHero} /><${CasesSection} pending=${pendingCase} onRead=${onRead} withExplorer=${true} /><${Footer} go=${go} openFind=${openFind} /></div>`}
    </main>
    ${quizOk && quizMounted && html`<div className="quiz-host" hidden=${route !== 'quiz'}><${window.QuizApp} active=${route === 'quiz'} /></div>`}
    ${reader && html`<${Reader} c=${reader} onClose=${() => setReader(null)} />`}
    <${ToastHost} />
    ${findOpen && html`<${Palette} onClose=${() => setFindOpen(false)} go=${go} openCase=${openCase} />`}
  <//>`;
}

ReactDOM.createRoot(document.getElementById('root')).render(html`<${App} />`);
})();
