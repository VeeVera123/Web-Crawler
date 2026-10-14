/* ==========================================================================
   HOME
   ========================================================================== */
const ORBIT = ART.plates.filter((u) => u !== ART.center);

function Hero({ go }) {
  const onMove = (e) => {
    const r = e.currentTarget.getBoundingClientRect();
    e.currentTarget.style.setProperty('--mx', ((e.clientX - r.left) / r.width - 0.5) * 2);
    e.currentTarget.style.setProperty('--my', ((e.clientY - r.top) / r.height - 0.5) * 2);
  };
  const n = ORBIT.length;
  return html`<section className="home-sec bs-hero" data-nav="light" onPointerMove=${onMove}>
    <div className="bs-ghost" aria-hidden="true">STEP 1</div>
    <div className="bs-hero-top fanfade-in">
      <p className="kquiet">A free place to practise for Step 1</p>
      <h1 className="bs-h">Dear <${NameField} />,<br /><em>you're amazing.</em></h1>
      <p className="bs-sub">Somewhere between the late nights and the cold coffee, you decided to become a doctor. This little room is free, and it's yours. Come and practise whenever you need to.</p>
      <div className="bs-hero-cta">
        <button className="btn-dark" onClick=${() => go('quiz')}>Start a quiz <${Icon} n="fa-arrow-right" /></button>
        <button className="btn-ghost" onClick=${() => go('library')}>Take a look around</button>
      </div>
    </div>
    <div className="orb-wrap" aria-hidden="true">
      <div className="orb">
        <div className="orb-center"><img src=${ART.center} alt="" onError=${hidePicture} /></div>
        ${ORBIT.map((src, i) => html`<div key=${i} className="orb-card" style=${{ '--a': ((i * 360) / n) + 'deg', '--ap': (i / n).toFixed(4), '--i': i }}>
          <div className="orb-img"><img src=${src} alt="" onError=${hidePicture} /></div></div>`)}
      </div>
    </div>
    <div className="scroll-hint">scroll</div>
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
  { k: 'Tools', title: 'Little helpers, close by', body: 'A calculator sits next to the question, never on top of it.', bg: '#123B39', cls: 'on-dark', ico: 'fa-calculator' },
  { k: 'Focus', title: 'Music to settle into', body: 'Pick a playlist, press play, and let the room go quiet around you.', bg: '#1C1A3B', cls: 'on-dark', ico: 'fa-music' },
  { k: 'Progress', title: 'Your progress stays yours', body: "Your scores live on this device, so you can see how far you've come.", art: 'angiogram', ico: 'fa-chart-line' },
  { k: 'Library', title: 'The stories behind the answers', body: 'Read a little deeper, save the pages you love, and come back to them.', bg: '#E9C65A', cls: 'on-light', ico: 'fa-bookmark' },
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

/* Sticky stacked cards: each one slides up and settles on top of the last. */
const STACK = [
  { n: '01', k: 'Show up', title: 'Start small. Just begin.', body: "Open a block, start the clock, and answer the first question. That's the whole job for today. The rest follows.", bg: '#111114', fg: '#F4F2EC', img: 'angiogram' },
  { n: '02', k: 'Learn the why', title: 'Read the reason behind every answer.', body: "Right or wrong, each question opens into an explanation with the real images beside it. That's where it really sticks.", bg: '#E7E0CF', fg: '#17150f', img: 'heartLungs' },
  { n: '03', k: 'Notice the pattern', title: 'See what keeps tripping you up.', body: 'Your dashboard keeps quiet track, so you can spot the topics that need another look. No grades, no judgement.', bg: '#1D2B4A', fg: '#F2F4F9', img: 'fingerprint' },
  { n: '04', k: 'Come back tomorrow', title: 'Be gentle. Be consistent.', body: 'A little each day beats a lot once. Keep your streak, save the pages you love, and let the work add up.', bg: '#F4A582', fg: '#2a140a', img: 'center' },
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
    { n: html`<em>${CASES.length}</em>`, l: 'cases to start with, and more on the way' },
    { n: '30:00', l: 'of quiet focus in every block' },
    { n: s.n ? html`${s.best}<em>%</em>` : html`<em>$0</em>`, l: s.n ? "your personal best so far. Look how far you've come." : 'because you already have enough to pay for' },
  ];
  return html`<section className="home-sec home-dark" data-nav="dark" style=${{ padding: 'clamp(70px,10vh,130px) clamp(18px,5vw,72px)', borderTop: '1px solid rgba(255,255,255,.07)' }}>
    <div className="home-wrap bs-stats">
      ${items.map((it, i) => html`<div key=${i} className="bs-stat reveal" style=${{ '--d': i * 90 + 'ms' }}><b>${it.n}</b><span>${it.l}</span></div>`)}
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
function QuoteBlock() {
  return html`<section className="home-sec home-dark qb" data-nav="dark">
    <div className="home-wrap">
      <div className="qb-grid">
        <blockquote className="qb-quote reveal"><p>The good physician treats the disease; the great physician treats the patient who has the disease.</p><cite>William Osler</cite></blockquote>
        <div className="qb-note reveal" style=${{ '--d': '120ms' }}>
          <p>Step 1 asks what you know. Everything after it asks who you are. So study hard, and be kind to yourself along the way, because the world needs the kind of doctor you're becoming.</p>
          <p className="tiny">Artwork: Caravaggio (1571–1610), Supper at Emmaus. The painting is in the public domain.</p>
        </div>
      </div>
      <div className="qb-giant-wrap reveal" aria-hidden="true"><div className="qb-giant" style=${{ backgroundImage: `url(${CARAV.fill})` }}>MEDICINE</div></div>
    </div>
  </section>`;
}
