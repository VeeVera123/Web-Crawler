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
async function publishStats() {
  const c = CloudStore.get(); if (!c.user || !(c.profile && c.profile.discoverable)) return;
  await sb.from('public_stats').upsert({ user_id: c.user.id, ...myStats(), updated_at: new Date().toISOString() });
}
async function setDiscoverable(on) {
  const c = CloudStore.get(); if (!c.user) return false;
  const { error } = await sb.from('profiles').update({ discoverable: on }).eq('id', c.user.id);
  if (error) { toast(/discoverable/i.test(error.message) ? 'Matching is not switched on for this project yet.' : error.message); return false; }
  patchCloud({ profile: { ...c.profile, discoverable: on } });
  if (on) await publishStats(); else await sb.from('public_stats').delete().eq('user_id', c.user.id);
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
  { sample: true, user_id: 's1', display_name: 'Amara', username: 'amara_n', study_year: 'MS2', country_name: 'Nigeria', score: 94, weekly_minutes: 1080, accuracy: 78, streak: 21, goal_accuracy: 80, bio: 'Cardiology block, flashcards every morning.' },
  { sample: true, user_id: 's2', display_name: 'Lucas', username: 'lucas_md', study_year: 'MS2', country_name: 'Brazil', score: 91, weekly_minutes: 900, accuracy: 74, streak: 12, goal_accuracy: 78, bio: 'Forty questions a day and a long walk after.' },
  { sample: true, user_id: 's3', display_name: 'Priya', username: 'priya_k', study_year: 'MS3', country_name: 'India', score: 88, weekly_minutes: 1440, accuracy: 81, streak: 33, goal_accuracy: 85, bio: 'Reviewing every missed question, weekly.' },
  { sample: true, user_id: 's4', display_name: 'Noah', username: 'noah_s', study_year: 'Gap year', country_name: 'Canada', score: 85, weekly_minutes: 720, accuracy: 70, streak: 9, goal_accuracy: 75, bio: 'Rebuilding fundamentals, one system at a time.' },
  { sample: true, user_id: 's5', display_name: 'Mia', username: 'mia_r', study_year: 'MS2', country_name: 'Australia', score: 82, weekly_minutes: 960, accuracy: 76, streak: 17, goal_accuracy: 80, bio: 'Quiet mornings, loud study playlists.' },
  { sample: true, user_id: 's6', display_name: 'Kofi', username: 'kofi_a', study_year: 'MS1', country_name: 'Ghana', score: 79, weekly_minutes: 600, accuracy: 68, streak: 6, goal_accuracy: 72, bio: 'Starting early so the end is calm.' },
];

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
      for (let r = 0; r < n; r++) for (let k = 0; k < n; k++) if (q.isDark(r, k)) { const x = pad + k * cell, y = pad + r * cell, rad = cell * 0.28; g.beginPath(); g.roundRect ? g.roundRect(x, y, cell + 0.4, cell + 0.4, rad) : g.rect(x, y, cell + 0.4, cell + 0.4); g.fill(); }
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
    <div className="qr-act"><button className="cn-btn" onClick=${() => copyText(link, 'Link copied')}><${Icon} n="fa-link" /> Copy link</button><button className="cn-btn ghost" onClick=${save}><${Icon} n="fa-download" /> Save image</button></div></div>`;
}

/* ---------- chat ---------- */
function Chat({ conn, onBack }) {
  const c = useCloud(), [msgs, setMsgs] = useState([]), [text, setText] = useState(''), box = useRef(null);
  useEffect(() => {
    let dead = false; let ch = null;
    sb.from('messages').select('id,sender,body,created_at').eq('conn_id', conn.conn_id).order('created_at', { ascending: true }).limit(300).then(({ data }) => { if (!dead && data) setMsgs(data); });
    try { ch = sb.channel('chat-' + conn.conn_id).on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'messages', filter: 'conn_id=eq.' + conn.conn_id }, (pl) => { setMsgs((m) => (m.some((x) => x.id === pl.new.id) ? m : [...m, pl.new])); }).subscribe(); } catch { /* realtime unavailable */ }
    return () => { dead = true; if (ch) sb.removeChannel(ch); };
  }, [conn.conn_id]);
  useEffect(() => { if (box.current) box.current.scrollTop = box.current.scrollHeight; }, [msgs.length]);
  const send = async (e) => {
    e.preventDefault(); const body = text.trim(); if (!body) return; setText('');
    const { data, error } = await sb.from('messages').insert({ conn_id: conn.conn_id, sender: c.user.id, body }).select('id,sender,body,created_at').single();
    if (error) { toast('Could not send that message.'); setText(body); } else setMsgs((m) => (m.some((x) => x.id === data.id) ? m : [...m, data]));
  };
  return html`<div className="cn-chat"><div className="cn-chat-top"><button className="cn-back" onClick=${onBack} aria-label="Back"><${Icon} n="fa-arrow-left" /></button><${Av} p=${conn} cls="cn-av sm" /><div><b>${conn.display_name}</b><small>@${conn.username}</small></div></div>
    <div className="cn-msgs" ref=${box}>${msgs.length === 0 && html`<p className="cn-hint">Say hello. Share a tip, a plan, or a question you are stuck on.</p>`}${msgs.map((m) => html`<div key=${m.id} className=${cx('cn-msg', m.sender === c.user.id && 'me')}><span>${m.body}</span><i>${new Date(m.created_at).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' })}</i></div>`)}</div>
    <form className="cn-send" onSubmit=${send}><input value=${text} onChange=${(e) => setText(e.target.value)} placeholder="Write a message" maxLength="2000" aria-label="Message" /><button type="submit" aria-label="Send"><${Icon} n="fa-arrow-up" /></button></form></div>`;
}

/* ---------- the profile sheet ---------- */
function PeekSheet({ p, state, onBack, onConnect }) {
  const hrs = p.weekly_minutes != null ? Math.round((p.weekly_minutes / 60) * 10) / 10 : null;
  const why = whyChips(p);
  return html`<div className="cn-sheet"><div className="cn-hero-card"><${Av} p=${p} cls="cn-hero-av" /><button className="cn-back" onClick=${onBack} aria-label="Back"><${Icon} n="fa-arrow-left" /></button>
      ${p.score != null && html`<span className="cn-score-pill">${p.score}% match</span>`}</div>
    <div className="cn-sheet-body"><small className="cn-lab">${p.sample ? 'SAMPLE STUDENT' : 'STUDENT'}</small><h2>${p.display_name}</h2><p className="cn-meta">@${p.username}${p.study_year ? ' · ' + p.study_year : ''}${p.country_name ? ' · ' + p.country_name : ''}</p>
      ${why.length > 0 && html`<div className="cn-chips">${why.map((w) => html`<span key=${w}>${w}</span>`)}</div>`}
      <div className="cn-stats">${[['Hours a week', hrs != null ? hrs : '–'], ['Accuracy', p.accuracy != null ? p.accuracy + '%' : '–'], ['Streak', p.streak != null ? p.streak + 'd' : '–'], ['Exam', p.exam_date ? new Date(p.exam_date).toLocaleDateString(undefined, { month: 'short', year: '2-digit' }) : '–']].map(([k, v]) => html`<div key=${k}><b>${v}</b><span>${k}</span></div>`)}</div>
      ${p.bio && html`<div className="cn-about"><small>ABOUT</small><p>${p.bio}</p></div>`}
      <button className="cn-cta" disabled=${state === 'sent' || state === 'connected'} onClick=${() => onConnect(p)}>${state === 'sent' ? 'Request sent' : state === 'connected' ? 'Connected' : 'Connect'} <${Icon} n="fa-arrow-right" /></button></div></div>`;
}

/* ---------- the connect layer ---------- */
function ConnectUI({ close, deepUser }) {
  const c = useCloud(), name = useUserName();
  const [tab, setTab] = useState('matches');
  const [matches, setMatches] = useState(null), [people, setPeople] = useState([]), [results, setResults] = useState([]), [q, setQ] = useState('');
  const [sheet, setSheet] = useState(null), [chat, setChat] = useState(null), [sent, setSent] = useState({});
  const on = !!(c.profile && c.profile.discoverable), hasName = !!(c.profile && c.profile.username);
  const loadMatches = useCallback(async () => { if (!sb || !CloudStore.get().user) return; await publishStats(); const { data, error } = await sb.rpc('find_matches', { max_rows: 18 }); setMatches(error ? [] : data || []); }, []);
  const loadPeople = useCallback(async () => { if (!sb || !CloudStore.get().user) return; const { data } = await sb.rpc('my_connections'); setPeople(data || []); }, []);
  useEffect(() => { if (c.user && on) { loadMatches(); } else setMatches([]); loadPeople(); }, [c.user && c.user.id, on]);
  useEffect(() => { if (!deepUser || !c.user) return; sb.rpc('public_profile', { u: deepUser }).then(({ data }) => { if (data && data[0]) setSheet(data[0]); else toast('That student is not available.'); }); }, [deepUser, c.user && c.user.id]);
  useEffect(() => {
    if (tab !== 'find' || q.trim().length < 2 || !c.user) { setResults([]); return undefined; }
    let live = true; const t = setTimeout(async () => { const { data } = await sb.rpc('search_users', { q: q.trim().replace(/^@/, '') }); if (live) setResults(data || []); }, 300);
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
  const list = real || (on || !c.user ? SAMPLE_MATCHES : []);
  const titles = { matches: 'Matches', find: 'Find', people: 'People', code: 'My code' };
  const Row = ({ p }) => html`<button className="cn-row" onClick=${() => setSheet(p)}><${Av} p=${p} cls="cn-av" /><div className="cn-row-t"><b>${p.display_name}</b><small>@${p.username}${p.study_year ? ' · ' + p.study_year : ''}${p.country_name ? ' · ' + p.country_name : ''}</small>${p.score != null && html`<div className="cn-chips">${whyChips(p).map((w) => html`<span key=${w}>${w}</span>`)}</div>`}</div>${p.score != null ? html`<span className="cn-pct">${p.score}%</span>` : ''}</button>`;
  return html`<div className="cn-ui"><div className="cn-wrap">
    ${chat ? html`<${Chat} conn=${chat} onBack=${() => setChat(null)} />` : sheet ? html`<${PeekSheet} p=${sheet} state=${stateOf(sheet)} onBack=${() => setSheet(null)} onConnect=${connect} />` : html`<${Fragment}>
      <div className="cn-top"><button className="cn-back" onClick=${close} aria-label="Close"><${Icon} n="fa-arrow-left" /></button><small className="cn-lab">${tab === 'matches' ? 'TODAY' : 'CONNECT'}</small></div>
      <h1 className="cn-h">${titles[tab]}</h1>
      ${tab === 'matches' && html`<${Fragment}>
        ${!c.user && html`<div className="cn-gate"><b>Meet students like you</b><p>Sign in to be matched by study time, scores, goals and exam date. Here is a preview with sample students.</p><button className="cn-btn" onClick=${() => { patchCloud({ modal: true }); }}>Sign in</button></div>`}
        ${c.user && !on && html`<div className="cn-gate"><b>Turn on matching</b><p>This shares a small summary with students you could match with: your username, photo, bio, weekly study time, score range, streak, exam month and goals. Never your notes or answers. You can switch it off any time.</p><button className="cn-btn" onClick=${turnOn}>${hasName ? 'Turn on matching' : 'Choose a username first'}</button></div>`}
        ${list.length > 0 && html`<${Fragment}>
          <div className="cn-grid">${list.slice(0, 6).map((p) => html`<button key=${p.user_id} className="cn-tile" onClick=${() => setSheet(p)}><${Av} p=${p} cls="cn-tile-av" /><span className="cn-tile-n">${p.display_name}</span><span className="cn-tile-s">${p.score}%</span></button>`)}</div>
          <small className="cn-lab">${real ? 'BEST FITS' : 'SAMPLE STUDENTS · PREVIEW'}</small><div className="cn-list">${list.map((p) => html`<${Row} key=${p.user_id} p=${p} />`)}</div><//>`}
        ${c.user && on && !real && matches && html`<p className="cn-hint">No real matches yet. As more students switch matching on, the best fits show up here. The students below are samples.</p>`}<//>`}
      ${tab === 'find' && html`<${Fragment}><label className="cn-search"><${Icon} n="fa-magnifying-glass" /><input value=${q} onChange=${(e) => setQ(e.target.value)} placeholder="Search a username" aria-label="Search a username" autoCapitalize="none" /></label>
        ${!c.user && html`<p className="cn-hint">Sign in to search students by username.</p>`}
        ${q.trim().length >= 2 && results.length === 0 && c.user && html`<p className="cn-hint">No student with that username is available.</p>`}
        <div className="cn-list">${results.map((p) => html`<${Row} key=${p.user_id} p=${p} />`)}</div><//>`}
      ${tab === 'people' && html`<${Fragment}>
        ${!c.user && html`<p className="cn-hint">Sign in to see your connections.</p>`}
        ${incoming.length > 0 && html`<${Fragment}><small className="cn-lab">REQUESTS</small><div className="cn-list">${incoming.map((x) => html`<div key=${x.conn_id} className="cn-row static"><${Av} p=${x} cls="cn-av" /><div className="cn-row-t"><b>${x.display_name}</b><small>@${x.username} wants to connect</small></div><button className="cn-mini" onClick=${() => accept(x)}>Accept</button><button className="cn-mini ghost" onClick=${() => decline(x)}>Not now</button></div>`)}</div><//>`}
        <small className="cn-lab">CONNECTED</small>${connected.length === 0 ? html`<p className="cn-hint">No connections yet. Send a request from your matches.</p>` : html`<div className="cn-list">${connected.map((x) => html`<button key=${x.conn_id} className="cn-row" onClick=${() => setChat(x)}><${Av} p=${x} cls="cn-av" /><div className="cn-row-t"><b>${x.display_name}</b><small>${x.last_body || 'Say hello'}</small></div><${Icon} n="fa-message" /></button>`)}</div>`}
        ${people.some((x) => x.status === 'pending' && x.i_requested) && html`<small className="cn-lab">WAITING</small><div className="cn-list">${people.filter((x) => x.status === 'pending' && x.i_requested).map((x) => html`<div key=${x.conn_id} className="cn-row static"><${Av} p=${x} cls="cn-av" /><div className="cn-row-t"><b>${x.display_name}</b><small>Request sent</small></div><button className="cn-mini ghost" onClick=${() => decline(x)}>Cancel</button></div>`)}</div>`}<//>`}
      ${tab === 'code' && html`<${QrCard} username=${c.profile && c.profile.username} />`}
      <nav className="cn-tabs" aria-label="Connect">${[['matches', 'fa-compass'], ['find', 'fa-magnifying-glass'], ['people', 'fa-user-group'], ['code', 'fa-qrcode']].map(([k, ic]) => html`<button key=${k} className=${cx(tab === k && 'on')} onClick=${() => setTab(k)} aria-label=${titles[k]}><${Icon} n=${ic} />${k === 'people' && incoming.length > 0 && html`<i>${incoming.length}</i>`}</button>`)}</nav><//>`}
  </div></div>`;
}
function ConnectLayer({ deepUser }) {
  const [stage, setStage] = useState('closed'), [o, setO] = useState({ x: 56, y: 40 }), btn = useRef(null);
  const open = (from) => { const r = from && btn.current ? btn.current.getBoundingClientRect() : null; setO(r ? { x: r.left + r.width / 2, y: r.top + r.height / 2 } : { x: 56, y: 40 }); setStage('opening'); requestAnimationFrame(() => requestAnimationFrame(() => setStage('open'))); };
  const close = () => { setStage('closing'); setTimeout(() => setStage('closed'), 1000); };
  useEffect(() => { if (deepUser && stage === 'closed') open(false); }, [deepUser]);
  useEffect(() => { if (stage === 'closed') return undefined; document.body.style.overflow = 'hidden'; const k = (e) => { if (e.key === 'Escape') close(); }; window.addEventListener('keydown', k); return () => { document.body.style.overflow = ''; window.removeEventListener('keydown', k); }; }, [stage]);
  return html`<${Fragment}>
    <button ref=${btn} className="cn-plus" onClick=${() => open(true)} aria-label="Connect with students"><svg viewBox="0 0 24 24" width="26" height="26" fill="none" stroke="#fff" strokeWidth="2.8" strokeLinecap="round"><path d="M12 5v14M5 12h14"/></svg></button>
    ${stage !== 'closed' && ReactDOM.createPortal(html`<div className=${cx('cn-root', stage)} style=${{ '--ox': o.x + 'px', '--oy': o.y + 'px' }}><i className="cn-ring r1"></i><i className="cn-ring r2"></i><i className="cn-ring r3"></i>${(stage === 'open' || stage === 'closing') && html`<${ConnectUI} close=${close} deepUser=${deepUser} />`}</div>`, document.body)}
  <//>`;
}

