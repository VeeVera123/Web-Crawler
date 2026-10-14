/* ==========================================================================
   CLOUD: Supabase accounts + sync. Everything keeps working offline and on
   this device; signing in just keeps your data in step across devices.
   ========================================================================== */
const SB_URL = 'https://womakbkesvwzwfviwlgs.supabase.co';
const SB_KEY = 'sb_publishable_lAY6AW3Ho6ZlWy83ox3FgQ_Znh_mj-G';     /* publishable key: safe in the browser, row-level security protects the data */
const sb = window.supabase ? window.supabase.createClient(SB_URL, SB_KEY, { auth: { persistSession: true, autoRefreshToken: true } }) : null;
const memStore = (init) => { let v = init; const subs = new Set(); return { get: () => v, set(n) { v = n; subs.forEach((f) => f()); }, subscribe(fn) { subs.add(fn); return () => subs.delete(fn); } }; };
const CloudStore = memStore({ user: null, profile: null, status: 'out', modal: false, error: '' });
const patchCloud = (p) => CloudStore.set({ ...CloudStore.get(), ...p });
const useCloud = () => useSyncExternalStore(CloudStore.subscribe, CloudStore.get);

const unionBy = (idf, prefer) => (a, b) => { const m = new Map(); [...a, ...b].forEach((x) => { const k = idf(x), o = m.get(k); m.set(k, o ? (prefer ? prefer(o, x) : o) : x); }); return [...m.values()]; };
const SYNC = {
  attempts: { st: window.AttemptStore, merge: (l, r) => unionBy((a) => a.timestamp)(l, r).sort((a, b) => a.timestamp - b.timestamp).slice(-200) },
  saved: { st: SavedStore, merge: unionBy((x) => x.url) },
  annotations: { st: AnnStore, merge: (l, r) => {
    const out = { ...r };
    Object.entries(l).forEach(([u, a]) => { const b = out[u]; out[u] = !b ? a : { ...b, ...a, hl: unionBy((h) => (typeof h === 'string' ? h : h.t))(hlList(a), hlList(b)), note: a.note || b.note }; });
    return out; } },
  notes: { st: NotesStore, merge: unionBy((n) => n.id, (o, x) => (x.updated > o.updated ? x : o)) },
  cards: { st: CardStore, merge: unionBy((c) => c.id, (o, x) => (x.reps > o.reps ? x : o)) },
  focus: { st: FocusLog, merge: (l, r) => unionBy((f) => f.at)(l, r).sort((a, b) => a.at - b.at).slice(-500) },
  mastered: { st: MasteredStore, merge: (l, r) => [...new Set([...l, ...r])] },
  plan: { st: PlanStore, lww: true },
  name: { st: UserStore, lww: true },
};
const TS_KEY = 'usmle-site:sync-ts';
const tsAll = () => { try { return JSON.parse(localStorage.getItem(TS_KEY) || '{}'); } catch { return {}; } };
const tsMark = (k) => { try { localStorage.setItem(TS_KEY, JSON.stringify({ ...tsAll(), [k]: Date.now() })); } catch { /* ignore */ } };
const isBlank = (v) => v == null || v === '' || (Array.isArray(v) && !v.length) || (typeof v === 'object' && !Array.isArray(v) && !Object.keys(v).length);
let applying = false, warned = false; const timers = {};

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
      const same = r && JSON.stringify(next) === JSON.stringify(r.value);
      if (!same && !isBlank(next) && (!def.lww || touched || r)) rows.push({ user_id: userId, key: k, value: next, updated_at: new Date().toISOString() });
    }
  } finally { applying = false; }
  if (rows.length) { const { error: e2 } = await sb.from('user_state').upsert(rows); if (e2) throw e2; }
  patchCloud({ status: 'synced' });
}
async function pushKey(k) {
  const u = CloudStore.get().user; if (!u || !sb) return;
  patchCloud({ status: 'syncing' });
  const { error } = await sb.from('user_state').upsert({ user_id: u.id, key: k, value: SYNC[k].st.get(), updated_at: new Date().toISOString() });
  if (error) { patchCloud({ status: 'error', error: error.message }); if (!warned) { warned = true; toast('Could not sync just now. Your data is safe on this device.'); } } else patchCloud({ status: 'synced' });
}
function startCloud() {
  if (startCloud.done) return; startCloud.done = true;
  Object.entries(SYNC).forEach(([k, def]) => def.st.subscribe(() => { if (applying) return; tsMark(k); if (CloudStore.get().user) { clearTimeout(timers[k]); timers[k] = setTimeout(() => pushKey(k), 1500); } }));
  if (!sb) return;
  const hydrate = async (session) => {
    const user = session && session.user; patchCloud({ user: user || null });
    if (!user) { patchCloud({ profile: null, status: 'out' }); return; }
    try {
      await pullAndMerge(user.id);
      const { data: prof } = await sb.from('profiles').select('display_name,is_admin').eq('id', user.id).maybeSingle();
      patchCloud({ profile: prof || null });
      if (prof && prof.display_name && !tsAll().name && UserStore.get() === 'Charles') { applying = true; UserStore.set(prof.display_name); applying = false; }
      sb.from('profiles').update({ last_seen: new Date().toISOString() }).eq('id', user.id).then(() => {});
    } catch (e) { patchCloud({ status: 'error', error: String(e.message || e) }); }
  };
  sb.auth.onAuthStateChange((_ev, session) => { setTimeout(() => hydrate(session), 0); });
  document.addEventListener('visibilitychange', () => { const u = CloudStore.get().user; if (document.visibilityState === 'visible' && u && CloudStore.get().status !== 'syncing') pullAndMerge(u.id).catch(() => {}); });
}
const Cloud = {
  async signUp(email, password, name) { const { data, error } = await sb.auth.signUp({ email, password, options: { data: { name } } }); if (error) throw error; return data; },
  async signIn(email, password) { const { error } = await sb.auth.signInWithPassword({ email, password }); if (error) throw error; },
  async github() { const { error } = await sb.auth.signInWithOAuth({ provider: 'github', options: { redirectTo: location.href.split('#')[0] } }); if (error) throw error; },
  async reset(email) { const { error } = await sb.auth.resetPasswordForEmail(email, { redirectTo: location.href.split('#')[0] }); if (error) throw error; },
  async out(wipe) { await sb.auth.signOut(); if (wipe) { Object.keys(localStorage).filter((k) => /^usmle-(site|quiz):/.test(k)).forEach((k) => localStorage.removeItem(k)); location.reload(); } },
};

function AuthModal() {
  const c = useCloud();
  const [mode, setMode] = useState('in');
  const [email, setEmail] = useState(''), [pw, setPw] = useState(''), [nm, setNm] = useState(UserStore.get());
  const [busy, setBusy] = useState(false), [err, setErr] = useState(''), [note, setNote] = useState('');
  useEffect(() => { if (c.user && c.modal) patchCloud({ modal: false }); }, [c.user, c.modal]);
  useEffect(() => {
    if (!c.modal) return undefined;
    const k = (e) => { if (e.key === 'Escape') patchCloud({ modal: false }); };
    window.addEventListener('keydown', k); return () => window.removeEventListener('keydown', k);
  }, [c.modal]);
  if (!c.modal) return null;
  const friendly = (m) => (/Invalid login/i.test(m) ? 'That email and password do not match.' : /already registered/i.test(m) ? 'There is already an account with that email. Try signing in.' : /provider is not enabled|Unsupported provider/i.test(m) ? 'GitHub sign-in is not switched on yet. Use email for now.' : /Password should/i.test(m) ? 'Please use a password with at least 6 characters.' : /rate limit/i.test(m) ? 'Too many tries. Please wait a minute.' : m);
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
      ${mode !== 'reset' && html`<button type="button" className="au-gh" onClick=${async () => { setErr(''); try { await Cloud.github(); } catch (x) { setErr(friendly(String(x.message || x))); } }}><${Icon} n="fa-github" cls="fab" /> Continue with GitHub</button><div className="au-or"><span>or</span></div>`}
      ${mode === 'up' && html`<label>Your name<input value=${nm} onChange=${(e) => setNm(e.target.value)} maxLength="24" autoComplete="name" /></label>`}
      <label>Email<input type="email" required value=${email} onChange=${(e) => setEmail(e.target.value)} autoComplete="email" /></label>
      ${mode !== 'reset' && html`<label>Password<input type="password" required minLength="6" value=${pw} onChange=${(e) => setPw(e.target.value)} autoComplete=${mode === 'up' ? 'new-password' : 'current-password'} /></label>`}
      ${err && html`<p className="au-err" role="alert">${err}</p>`}${note && html`<p className="au-note">${note}</p>`}
      <button className="au-go" type="submit" disabled=${busy}>${busy ? 'One moment…' : mode === 'up' ? 'Create account' : mode === 'reset' ? 'Send link' : 'Sign in'}</button>
      <div className="au-sw">${mode === 'in' ? html`<button type="button" onClick=${() => { setMode('up'); setErr(''); }}>New here? Create an account</button><button type="button" onClick=${() => { setMode('reset'); setErr(''); }}>Forgot password?</button>` : html`<button type="button" onClick=${() => { setMode('in'); setErr(''); setNote(''); }}>I already have an account</button>`}</div>
    </form></div>`, document.body);
}

function AccountButton() {
  const c = useCloud(), name = useUserName();
  const [open, setOpen] = useState(false);
  useEffect(() => {
    if (!open) return undefined;
    const d = (e) => { if (!e.target.closest('.acct-wrap')) setOpen(false); };
    document.addEventListener('mousedown', d); return () => document.removeEventListener('mousedown', d);
  }, [open]);
  if (!sb) return null;
  if (!c.user) return html`<button className="acct-in glass" onClick=${() => patchCloud({ modal: true })}><${Icon} n="fa-user" /><span className="lbl">Sign in</span></button>`;
  const label = { synced: 'All saved to your account', syncing: 'Saving…', error: 'Could not sync. Data is safe on this device.', out: '' }[c.status];
  return html`<div className="acct-wrap"><button className="acct-av glass" onClick=${() => setOpen((o) => !o)} aria-label="Account" aria-expanded=${open}><b>${(name || '?').trim().charAt(0).toUpperCase()}</b><i className=${'sd ' + c.status}></i></button>
    ${open && html`<div className="acct-menu"><div className="am-top"><b>${name}</b><small>${c.user.email}</small></div>
      <p className=${'am-st ' + c.status}><i className=${'sd ' + c.status}></i>${label}</p>
      <button onClick=${() => { pullAndMerge(c.user.id).catch(() => {}); }}><${Icon} n="fa-arrows-rotate" /> Sync now</button>
      <button onClick=${() => { setOpen(false); Cloud.out(false); }}><${Icon} n="fa-arrow-right-from-bracket" /> Sign out</button>
      <button onClick=${() => { if (confirm('Sign out and remove this device’s copy of your data? It stays safe in your account.')) Cloud.out(true); }}><${Icon} n="fa-eraser" /> Sign out and clear this device</button></div>`}</div>`;
}

