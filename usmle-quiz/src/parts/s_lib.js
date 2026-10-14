/* ==========================================================================
   LIBRARY: Wikipedia explorer with stacked cards, bookmarks, notes and a reader
   ========================================================================== */
const SUGGEST = ['Prostate', 'Finasteride', 'Tamsulosin', 'Spermatic cord', 'Bupropion', 'Brainstem', 'Omeprazole', 'Spleen'];

function WikiCard({ page, saved, onRead, i, showNote }) {
  return html`<article className="wk" style=${{ '--i': i }}>
    <div className="wk-card">
      <div className="wk-img">
        ${page.thumb ? html`<img src=${page.thumb} alt="" loading="lazy" onError=${hidePicture} />` : html`<div className="ph">${page.title.charAt(0)}</div>`}
        <button className=${cx('wk-save', saved && 'on')} onClick=${() => toggleSaved(page)} aria-pressed=${saved} aria-label=${saved ? 'Remove from saved' : 'Save this page'} title=${saved ? 'Saved' : 'Save for later'}>
          <${Icon} n=${saved ? 'fa-bookmark' : 'fa-bookmark'} cls=${saved ? 'fas' : 'far'} /></button>
      </div>
      <div className="wk-body"><h4>${page.title}</h4><p>${page.extract}</p></div>
      ${showNote && html`<textarea className="wk-note" placeholder="A note to your future self…" value=${page.note || ''} onChange=${(e) => setSavedNote(page.url, e.target.value)} aria-label=${`Note on ${page.title}`}></textarea>`}
      <div className="wk-foot">
        <button className="read" onClick=${() => onRead(page)}>Read here</button>
        <a href=${page.url} target="_blank" rel="noopener noreferrer">Wikipedia <${Icon} n="fa-arrow-up-right-from-square" /></a>
        <button className="ico" onClick=${() => copyText(page.url, 'Link copied')} aria-label="Copy link" title="Copy link"><${Icon} n="fa-link" /></button>
      </div>
    </div>
  </article>`;
}

function WikiReader({ page, onClose }) {
  const [full, setFull] = useState(null);
  const saved = useSaved();
  useEffect(() => {
    const k = (e) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', k); document.body.style.overflow = 'hidden';
    return () => { window.removeEventListener('keydown', k); document.body.style.overflow = ''; };
  }, [onClose]);
  useEffect(() => { let live = true; wikiFull(page.title).then((d) => live && setFull(d)).catch(() => {}); return () => { live = false; }; }, [page.title]);
  const text = (full && full.extract) || page.extract;
  const img = (full && full.thumb) || page.thumb;
  const on = isSaved(saved, page.url);
  return html`<div className="reader" onClick=${(e) => e.target === e.currentTarget && onClose()} role="dialog" aria-modal="true" aria-label=${page.title}>
    <div className="reader-card narrow"><div className="reader-top"><span>Wikipedia · quick read</span><button onClick=${onClose} aria-label="Close"><${Icon} n="fa-xmark" /></button></div>
      <div className="reader-body">
        <div>
          ${img && html`<img className="wk-hero" src=${img} alt="" onError=${hidePicture} />`}
          <h3>${page.title}</h3>
          ${text.split(/\n+/).map((p, k) => html`<p key=${k}>${p}</p>`)}
          <div className="rd-acts">
            <button className=${on ? 'primary' : ''} onClick=${() => toggleSaved(page)}><${Icon} n="fa-bookmark" cls=${on ? 'fas' : 'far'} />${on ? 'Saved' : 'Save for later'}</button>
            <a href=${page.url} target="_blank" rel="noopener noreferrer">Read the full article <${Icon} n="fa-arrow-up-right-from-square" /></a>
            <button onClick=${() => copyText(page.url, 'Link copied')}><${Icon} n="fa-link" />Copy link</button>
          </div>
        </div>
      </div></div></div>`;
}

function Explorer() {
  const [q, setQ] = useState('');
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
      wikiSearch(q, ctl.signal).then((r) => { setRes(r); setState(r.length ? 'ok' : 'none'); })
        .catch((e) => { if (e.name !== 'AbortError') setState('error'); });
    }, 350);
    return () => { clearTimeout(t); ctl.abort(); };
  }, [q, nonce]);

  const pick = (term) => { LibTab.set('results'); setQ(term); };
  const exportList = () => copyText(saved.map((p) => `• ${p.title} — ${p.url}${p.note ? `\n   ${p.note}` : ''}`).join('\n'), 'Your list is copied');
  const clearAll = () => { if (confirm('Remove every saved page?')) { SavedStore.set([]); toast('Cleared'); } };

  return html`<div className="explorer">
    <div className="ex-head reveal"><p className="eyebrow">Go deeper</p><h2>Curious about something? <span className="hl">Look it up.</span></h2>
      <p>Search any drug, structure or disease. Read a quick summary here, save the pages you love, and add a note for later.</p></div>
    <label className="ex-input frost reveal" style=${{ '--d': '80ms' }}><${Icon} n="fa-magnifying-glass" /><input value=${q} onChange=${(e) => { LibTab.set('results'); setQ(e.target.value); }} placeholder="Try “finasteride” or “brainstem”…" aria-label="Search Wikipedia" />
      ${q && html`<button className="ex-x" onClick=${() => setQ('')} aria-label="Clear"><${Icon} n="fa-xmark" /></button>`}</label>
    <div className="ex-row">
      <div className="ex-chips">${SUGGEST.map((t) => html`<button key=${t} onClick=${() => pick(t)}>${t}</button>`)}</div>
      <${Seg} value=${tab} onChange=${(v) => LibTab.set(v)} options=${[['results', 'Search'], ['saved', `Saved${saved.length ? ' · ' + saved.length : ''}`]]} />
    </div>

    ${tab === 'results' && html`<${Fragment}>
      ${state === 'idle' && html`<p className="ex-msg">Type a topic above, or tap one of the ideas. We'll pull up the page and you can save anything worth keeping.</p>`}
      ${state === 'none' && html`<p className="ex-msg">We couldn't find anything for “${q}”. A simpler word might help.</p>`}
      ${state === 'error' && html`<p className="ex-msg">We couldn't reach Wikipedia just now. Check your connection, then <button className="btn-ghost btn-sm" onClick=${() => setNonce((n) => n + 1)}>try again</button></p>`}
      <div className="ex-grid">
        ${state === 'loading' && [0, 1, 2, 3, 4, 5].map((k) => html`<div key=${k} className="wk sk"><div className="wk-card"></div></div>`)}
        ${state !== 'loading' && res.map((p, i) => html`<${WikiCard} key=${p.url} page=${p} i=${i} saved=${isSaved(saved, p.url)} onRead=${setReading} />`)}
      </div><//>`}

    ${tab === 'saved' && html`<${Fragment}>
      ${saved.length > 0 && html`<div className="ex-tools"><button className="btn-ghost btn-sm" onClick=${exportList}><${Icon} n="fa-copy" />Copy my list</button><button className="btn-ghost btn-sm" onClick=${clearAll}><${Icon} n="fa-trash-can" />Clear all</button></div>`}
      ${saved.length === 0 && html`<p className="ex-msg">Nothing saved yet. Tap the bookmark on any page and it will wait for you right here.</p>`}
      <div className="ex-grid">${saved.map((p, i) => html`<${WikiCard} key=${p.url} page=${p} i=${i} saved=${true} showNote=${true} onRead=${setReading} />`)}</div><//>`}

    ${reading && html`<${WikiReader} page=${reading} onClose=${() => setReading(null)} />`}
  </div>`;
}
