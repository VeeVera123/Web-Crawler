/* ==========================================================================
   STUDY: notes with a sketch pad, spaced-repetition cards, mistake notebook,
   planner and a pomodoro timer. Everything is saved on this device.
   ========================================================================== */
const NotesStore = makeStore('usmle-site:notes', []);
const CardStore = makeStore('usmle-site:cards', []);
const PlanStore = makeStore('usmle-site:plan', { exam: '', goal: 20, tasks: [] });
const FocusLog = makeStore('usmle-site:focus', []);                 /* [{ at, sec }] finished focus blocks */
const MasteredStore = makeStore('usmle-site:mastered', []);
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
  const [hasBg, setHasBg] = useState(!!note.bg);
  const loadImg = (src, cross) => new Promise((ok, no) => { const im = new Image(); if (cross) im.crossOrigin = 'anonymous'; im.onload = () => ok(im); im.onerror = no; im.src = src; });
  const paintBg = (im) => {
    const c = bgRef.current.getContext('2d'); c.fillStyle = '#fff'; c.fillRect(0, 0, SK_W, SK_H);
    if (im) { const k = Math.min(SK_W / im.width, SK_H / im.height); const w = im.width * k, h = im.height * k; c.drawImage(im, (SK_W - w) / 2, (SK_H - h) / 2, w, h); }
  };
  useEffect(() => {
    paintBg(null);
    if (note.bg) loadImg(note.bg).then(paintBg).catch(() => {});
    if (note.ink) loadImg(note.ink).then((im) => inkRef.current.getContext('2d').drawImage(im, 0, 0)).catch(() => {});
  }, []);
  const save = () => onInk({ ink: inkRef.current.toDataURL('image/png'), bg: bgData.current });
  const setBackground = async (src, cross) => {
    try {
      const im = await loadImg(src, cross); paintBg(im);
      bgData.current = bgRef.current.toDataURL('image/jpeg', 0.82); setHasBg(true); save();
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
  const clearAll = () => { if (!confirm('Clear the drawing and the background?')) return; undo.current = []; inkRef.current.getContext('2d').clearRect(0, 0, SK_W, SK_H); paintBg(null); bgData.current = ''; setHasBg(false); save(); };
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
      <input type="date" value=${plan.exam} onChange=${(e) => set({ exam: e.target.value })} aria-label="Exam date" /></div>
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
function beep() { try { const a = new (window.AudioContext || window.webkitAudioContext)(); [0, 0.22, 0.44].forEach((d) => { const o = a.createOscillator(), g = a.createGain(); o.frequency.value = 660 + d * 400; g.gain.value = 0.06; o.connect(g); g.connect(a.destination); o.start(a.currentTime + d); o.stop(a.currentTime + d + 0.15); }); } catch { /* no audio */ } }
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

