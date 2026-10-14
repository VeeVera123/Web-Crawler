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

function Globe({ world, marks, me, flight, onFlightEnd }) {
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
  const [rows, setRows] = useState([]), [mine, setMine] = useState(null);
  const [sample, setSample] = useState(true);
  const [busy, setBusy] = useState(false);
  const [flight, setFlight] = useState(null), [partner, setPartner] = useState(null);
  useEffect(() => { loadWorld().then(setWorld).catch((e) => setErr(String(e.message || e))); }, []);
  const refresh = useCallback(async () => {
    if (!sb || !CloudStore.get().user) { setRows([]); setMine(null); return; }
    const a = await sb.rpc('community_map', { min_count: 3 }); if (!a.error) setRows(a.data || []);
    const b = await sb.from('presence').select('country_id,country_name').eq('user_id', CloudStore.get().user.id).maybeSingle(); setMine(b.data || null);
  }, []);
  useEffect(() => { refresh(); }, [c.user && c.user.id]);
  const save = async (id, name) => { setBusy(true); const { error } = await sb.from('presence').upsert({ user_id: c.user.id, country_id: id, country_name: name }); setBusy(false); if (error) toast('Could not save. ' + error.message); else { toast('You are on the globe, as ' + name + ' only.'); refresh(); } };
  const remove = async () => { setBusy(true); await sb.from('presence').delete().eq('user_id', c.user.id); setBusy(false); setMine(null); toast('You are off the globe.'); refresh(); };
  const real = rows.map((r) => ({ id: r.country_id, n: Number(r.n), name: r.country_name }));
  const fake = sample && world ? SAMPLE_PEOPLE.map(([id, n]) => ({ id, n, name: world.names[id] || id })) : [];
  const marks = [...real, ...fake.filter((f) => !real.some((r) => r.id === f.id))];
  const total = marks.reduce((s, m) => s + m.n, 0);
  const match = () => {
    if (!mine) { toast('Pick your country first, and the match will fly from there.'); return; }
    const p = SAMPLE_PARTNERS[Math.floor(Math.random() * SAMPLE_PARTNERS.length)];
    setPartner(null); setFlight({ from: mine.country_id, to: p.id, key: Date.now() }); setTimeout(() => setPartner(p), 2400);
  };
  return html`<section className="cm" data-nav="dark">
    <div className="cm-head"><p className="eyebrow">Community</p><h1>Students, <span className="hl">everywhere.</span></h1>
      <p>A quiet look at where people are studying from. Only a country is ever shared, and only if you choose. No addresses, no cities, no names on the map.</p></div>
    <div className="cm-grid">
      <div className="cm-globe">${world ? html`<${Globe} world=${world} marks=${marks} me=${mine && mine.country_id} flight=${flight} onFlightEnd=${() => { setFlight(null); setPartner(null); }} />` : html`<div className="cm-load">${err ? 'The map could not load. Check your connection and refresh.' : html`<span></span>Loading the globe…`}</div>`}
        <div className="cm-legend"><i></i> ${total ? total + ' student' + (total === 1 ? '' : 's') + ' in ' + marks.length + ' countr' + (marks.length === 1 ? 'y' : 'ies') : 'No one on the map yet'}${sample && fake.length ? ' · includes sample data' : ''}</div></div>
      <div className="cm-side">
        <div className="cm-card"><small>Your place on the globe</small>
          ${!c.user ? html`<p>Sign in to choose whether to appear, and to see real counts.</p><button className="lh-btn" onClick=${() => patchCloud({ modal: true })}><${Icon} n="fa-user" /> Sign in</button>` : html`<${Fragment}>
            <p>${mine ? html`You show up as <b>${mine.country_name}</b>. Others only ever see how many students are in each country, and countries with fewer than three are hidden.` : 'Off by default. Choose your country to appear in the count. You can leave any time.'}</p>
            <${CountryPicker} world=${world} value=${mine && mine.country_id} onPick=${save} />
            ${mine && html`<button className="cm-off" onClick=${remove} disabled=${busy}><${Icon} n="fa-eye-slash" /> Take me off the globe</button>`}<//>`}</div>
        <div className="cm-card dark"><small>Find a study partner</small>
          <p>Soon, you will be matched with someone who shares your goals and keeps the same steady streak. Here is a preview with sample students.</p>
          <button className="lh-btn" onClick=${match} disabled=${!world}><${Icon} n="fa-wand-magic-sparkles" /> Preview a match</button>
          ${partner && html`<div className="cm-match"><b>${partner.name}</b><span>${world.names[partner.id]}</span><p>${partner.goal}</p><div><em>${partner.streak} day streak</em><em>${partner.hours} h a week</em></div><small>Sample student. Real matching is coming.</small></div>`}</div>
        <label className="cm-check"><input type="checkbox" checked=${sample} onChange=${(e) => setSample(e.target.checked)} /><span>Show sample students until the community grows</span></label>
      </div>
    </div>
  </section>`;
}

