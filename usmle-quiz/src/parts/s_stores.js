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
  if (isSaved(list, page.url)) { SavedStore.set(list.filter((p) => p.url !== page.url)); toast('Removed from your saved pages'); }
  else {
    SavedStore.set([{ title: page.title, extract: page.extract || '', thumb: page.thumb || '', url: page.url, note: '', at: Date.now() }, ...list]);
    toast('Saved. It will be waiting for you in the Library.');
  }
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
