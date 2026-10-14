/* ==========================================================================
   FOOTER  (Caravaggio, Supper at Emmaus as the backdrop)
   ========================================================================== */
function Footer({ go, openFind }) {
  const link = (r, label) => html`<button className="flink" onClick=${() => go(r)}>${label}</button>`;
  return html`<footer className="site-footer" data-nav="dark" style=${{ '--poster': `url(${CARAV.poster})` }}>
    <div className="sf-bg" aria-hidden="true"></div>
    <div className="sf-inner">
      <div className="cols">
        <div><div style=${{ display: 'flex', alignItems: 'center', gap: 10, fontWeight: 700, fontSize: 17 }}><span className="brand-mark"><i className="fas fa-plus"></i></span>USMLE Quiz</div>
          <p className="blurb">A free place to practise for Step 1, made with care for everyone on the long road to a white coat. You've got this.</p>
          <button className="btn-white" onClick=${() => go('quiz')}>Start a quiz <${Icon} n="fa-arrow-right" /></button></div>
        <div><h5>Wander</h5>${link('home', 'Home')}${link('quiz', 'Quiz')}${link('dashboard', 'Dashboard')}${link('library', 'Library')}</div>
        <div><h5>Little helpers</h5><button className="flink" onClick=${openFind}>Find anything · ⌘K</button>${link('quiz', 'Calculator')}${link('quiz', 'Study playlist')}${link('dashboard', 'Focus timer')}</div>
        <div><h5>Read more</h5>${CASES.slice(0, 5).map((c) => html`<a key=${c.id} href=${'https://en.wikipedia.org/wiki/' + encodeURIComponent(c.wiki.replace(/ /g, '_'))} target="_blank" rel="noopener noreferrer">${c.wiki} ↗</a>`)}</div>
      </div>
      <div className="footer-bar"><span>© ${new Date().getFullYear()} USMLE Quiz · Free, and staying that way. A practice tool, not a substitute for your curriculum.<br />Backdrop: Caravaggio, 1571–1610.</span>
        <button className="totop" onClick=${() => window.scrollTo({ top: 0, behavior: 'smooth' })} aria-label="Back to top"><${Icon} n="fa-arrow-up" /></button></div>
    </div>
  </footer>`;
}
