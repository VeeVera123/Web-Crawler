/* ==========================================================================
   DASHBOARD  (layout after your Donezo reference, wired to your real attempts)
   ========================================================================== */
const HATCH_H = [58, 76, 64, 88, 82, 56, 72];
const WEEKDAYS = ['S', 'M', 'T', 'W', 'T', 'F', 'S'];
const CASE_ICONS = ['fa-droplet', 'fa-pills', 'fa-user-doctor', 'fa-hand', 'fa-brain', 'fa-flask', 'fa-heart-pulse', 'fa-x-ray'];
const CASE_TINTS = ['#2f6fed', '#1f8a57', '#e8903a', '#d8a912', '#7a4fd0', '#e5586a', '#0f9aa8', '#6b7280'];
const AV_TINTS = ['#f6c9c3', '#cfe9c7', '#cdd3f5', '#f6e3b4', '#d4eaf0'];

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
    <defs><pattern id="dzHatch" width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="3" height="7" fill="rgba(20,60,40,.38)" /></pattern></defs>
    <path d="M24,110 A86,86 0 0 1 196,110" fill="none" stroke="#eef2ef" strokeWidth="34" strokeLinecap="round" />
    ${parts.filter((p) => p.v > 0).map((p, i) => {
      const seg = Math.max(0, (p.v / total) * LEN - 10), off = (acc / total) * LEN; acc += p.v;
      return html`<path key=${i} className="arc" d="M24,110 A86,86 0 0 1 196,110" stroke=${p.c} strokeDasharray=${`${seg} ${LEN}`} strokeDashoffset=${-off - 5} style=${{ '--len': LEN, animationDelay: i * 160 + 'ms' }} />`;
    })}
  </svg>`;
}

function FocusTimer() {
  const [running, setRunning] = useState(false);
  const [sec, setSec] = useState(0);
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
      <button className="go" onClick=${() => setRunning((r) => !r)} aria-label=${running ? 'Pause' : 'Start'}><${Icon} n=${running ? 'fa-pause' : 'fa-play'} /></button>
      <button className="stop" onClick=${() => { setRunning(false); setSec(0); }} aria-label="Reset"><${Icon} n="fa-stop" /></button>
    </div>
  </article>`;
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
                <${Gauge} parts=${[{ v: masteredN, c: '#0b5a3a' }, { v: learningN, c: '#3aa56f' }, { v: notYetN, c: 'url(#dzHatch)' }]} />
                <div className="mid"><b>${masteryPct}%</b><small>Mastered</small></div>
              </div>
              <div className="dz-legend"><span><i style=${{ background: '#0b5a3a' }}></i>Mastered</span><span><i style=${{ background: '#3aa56f' }}></i>Learning</span><span><i style=${{ background: 'repeating-linear-gradient(135deg,#7d9a8a 0 2px,#fff 2px 4px)' }}></i>Not yet</span></div>
            </article>

            <${FocusTimer} />
          </div>
        </div>
      </div>
    </div>
  </section>`;
}
