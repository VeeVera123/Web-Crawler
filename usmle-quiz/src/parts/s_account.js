/* ==========================================================================
   ACCOUNT: your profile home, progress, goals, settings and your data
   ========================================================================== */
function SelectBox({ value, options, onChange, placeholder = 'Choose', label }) {
  const [open, setOpen] = useState(false);
  const [pos, setPos] = useState(null);
  const [hi, setHi] = useState(-1);
  const btn = useRef(null);
  const place = useCallback(() => {
    if (!btn.current) return; const r = btn.current.getBoundingClientRect(), H = Math.min(options.length * 46 + 16, 340);
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
  const c = useCloud(), name = useUserName(), attempts = useAttempts().filter((a) => !a.sample), cards = useStore(CardStore), log = useStore(FocusLog), plan = useStore(PlanStore);
  const p = c.profile || {};
  const st = summarize(attempts);
  const days = plan.exam ? Math.ceil((new Date(plan.exam + 'T00:00:00') - new Date(new Date().toDateString())) / DAY) : null;
  if (!c.user) return html`<div className="ac-hero"><div className="ac-av"><b>${(name || '?').charAt(0).toUpperCase()}</b></div><div><h2>${name}</h2><p>Sign in to add a photo and username, and to keep your notes, cards and scores on every device.</p><button className="lh-btn" onClick=${() => patchCloud({ modal: true })}><${Icon} n="fa-user" /> Sign in or create an account</button></div></div>`;
  return html`<${Fragment}>
    <div className="ac-hero"><div className="ac-av">${p.avatar_url ? html`<img src=${p.avatar_url} alt="" />` : html`<b>${(name || '?').charAt(0).toUpperCase()}</b>`}</div>
      <div><h2>${name}</h2><small>${p.username ? '@' + p.username : 'No username yet'} · ${c.user.email}</small>
        <p>${p.bio || 'Add a line about you and your goals.'}</p>
        <div className="ac-chips">${p.study_year && html`<span>${p.study_year}</span>`}${days !== null && days >= 0 && html`<span>${days} days to Step 1</span>`}<span>Joined ${new Date(c.user.created_at || Date.now()).toLocaleDateString(undefined, { month: 'short', year: 'numeric' })}</span></div>
        <button className="lh-btn" onClick=${() => patchCloud({ profileOpen: true })}><${Icon} n="fa-user-pen" /> Edit profile</button></div></div>
    <div className="st-stats"><div><b>${st.n}</b><span>quizzes</span></div><div><b>${st.n ? st.best + '%' : '–'}</b><span>best score</span></div><div><b>${st.streak}</b><span>day streak</span></div><div><b>${cards.filter((x) => x.due <= Date.now()).length}</b><span>cards due</span></div><div><b>${focusToday(log)}</b><span>min focused today</span></div></div>
    <div className="ac-soon"><i className="fas fa-user-group"></i><div><b>Study buddies and the community map</b><p>Soon you will be able to find other students, and see roughly which regions people are studying from. Only regions, never addresses, and only if you choose to show up.</p></div><span>Coming soon</span></div>
  <//>`;
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

function SettingsTab() {
  const pr = useStore(PrefsStore);
  const set = (p) => PrefsStore.set({ ...PrefsStore.get(), ...p });
  const Row = ({ k, title, sub }) => html`<label className="ac-row"><div><b>${title}</b><small>${sub}</small></div><button type="button" role="switch" aria-checked=${!!pr[k]} className=${cx('ac-sw', pr[k] && 'on')} onClick=${() => set({ [k]: !pr[k] })}><i></i></button></label>`;
  useEffect(() => { document.body.classList.toggle('rm', !!pr.reduceMotion); }, [pr.reduceMotion]);
  return html`<div className="ac-list">
    <${Row} k="sound" title="Timer sounds" sub="A soft chime when a focus block or break ends." />
    <${Row} k="collapseNav" title="Fold the menu as you scroll" sub="The navigation shrinks into the stack button while you read." />
    <${Row} k="reduceMotion" title="Calmer motion" sub="Turns off most animation across the site." />
    <div className="ac-row"><div><b>Open the site on</b><small>The page you land on when you arrive.</small></div><div className="ac-sel"><${SelectBox} value=${pr.landing} onChange=${(v) => set({ landing: v })} label="Landing page" options=${[['home', 'Home'], ['quiz', 'Quiz'], ['dashboard', 'Dashboard'], ['study', 'Study'], ['library', 'Library']]} /></div></div>
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

