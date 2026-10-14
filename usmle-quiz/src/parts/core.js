/* ==========================================================================
   CORE: shared services used by both the quiz and the site shell.
   1. AttemptStore    - persistent quiz history (localStorage) other tabs can read.
   2. Jelly physics   - spring-driven tilt / squash / giggle for any element with .jelly
   3. Quiz protection - copy / right-click / blur guards, active only on the Quiz tab
   ========================================================================== */
(() => {
  'use strict';

  /* ---------- 1. AttemptStore ---------- */
  const KEY = 'usmle-quiz:attempts:v2';
  const read = () => {
    try {
      const raw = JSON.parse(localStorage.getItem(KEY) || '[]');
      return Array.isArray(raw) ? raw.filter(a => a && Number.isFinite(a.score) && Number.isFinite(a.total) && a.total > 0) : [];
    } catch { return []; }
  };
  let list = read();
  const subs = new Set();
  const emit = () => subs.forEach(fn => fn());
  window.AttemptStore = {
    get: () => list,                                   /* stable reference until it changes */
    subscribe(fn) { subs.add(fn); return () => subs.delete(fn); },
    add(attempt) {
      if (list.some(a => a.timestamp === attempt.timestamp)) return;
      list = [...list, attempt].slice(-200);
      try { localStorage.setItem(KEY, JSON.stringify(list)); } catch { /* storage unavailable */ }
      emit();
    },
    set(next) {
      list = Array.isArray(next) ? next : [];
      try { localStorage.setItem(KEY, JSON.stringify(list)); } catch { /* storage unavailable */ }
      emit();
    },
    clear() {
      list = [];
      try { localStorage.removeItem(KEY); } catch { /* ignore */ }
      emit();
    },
  };

  /* ---------- 2. Jelly physics ---------- */
  if (!window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
    const K = 230, D = 11;                       /* stiffness / damping: under-damped, so it wobbles */
    const states = new WeakMap();
    const live = new Set();
    let hovered = null, pressed = null, last = 0, raf = 0;

    const axis = (t = 0) => ({ p: t, v: 0, t });
    const stateOf = (el) => {
      let s = states.get(el);
      if (!s) {
        s = { x: axis(), y: axis(), z: axis(), s: axis(1),
              tilt: parseFloat(el.dataset.tilt || '6'),
              hover: parseFloat(el.dataset.hover || '1.04') };
        states.set(el, s);
      }
      return s;
    };
    const wake = (el) => { live.add(el); if (!raf) { last = performance.now(); raf = requestAnimationFrame(tick); } };

    function tick(now) {
      const dt = Math.min(0.032, (now - last) / 1000);
      last = now;
      for (const el of live) {
        const s = states.get(el);
        let moving = false;
        for (const k of ['x', 'y', 'z', 's']) {
          const a = s[k];
          a.v += (-K * (a.p - a.t) - D * a.v) * dt;
          a.p += a.v * dt;
          if (Math.abs(a.p - a.t) > 0.0006 || Math.abs(a.v) > 0.008) moving = true;
        }
        if (moving) {
          el.style.transform = `perspective(900px) rotateX(${s.x.p.toFixed(3)}deg) rotateY(${s.y.p.toFixed(3)}deg) rotateZ(${s.z.p.toFixed(3)}deg) scale(${s.s.p.toFixed(4)})`;
        } else {
          for (const k of ['x', 'y', 'z', 's']) { s[k].p = s[k].t; s[k].v = 0; }
          const neutral = !s.x.t && !s.y.t && !s.z.t && s.s.t === 1;
          el.style.transform = neutral ? '' : `perspective(900px) rotateX(${s.x.t}deg) rotateY(${s.y.t}deg) scale(${s.s.t})`;
          live.delete(el);
        }
      }
      raf = live.size ? requestAnimationFrame(tick) : 0;
    }

    const rest = (el) => { const s = stateOf(el); s.x.t = 0; s.y.t = 0; s.z.t = 0; s.s.t = 1; wake(el); };
    const closest = (e) => (e.target && e.target.closest) ? e.target.closest('.jelly') : null;

    document.addEventListener('pointerover', (e) => {
      if (e.pointerType !== 'mouse') return;
      const el = closest(e);
      if (!el || el === hovered || el.disabled) return;
      hovered = el;
      const s = stateOf(el);
      s.s.t = s.hover;
      s.z.v += (Math.random() < 0.5 ? -1 : 1) * 42;        /* the giggle */
      wake(el);
    });
    document.addEventListener('pointermove', (e) => {
      if (!hovered || e.pointerType !== 'mouse') return;
      const r = hovered.getBoundingClientRect();
      if (!r.width || !r.height) return;
      const s = stateOf(hovered);
      s.y.t = ((e.clientX - r.left) / r.width - 0.5) * 2 * s.tilt;
      s.x.t = -((e.clientY - r.top) / r.height - 0.5) * 2 * s.tilt;
      wake(hovered);
    }, { passive: true });
    document.addEventListener('pointerout', (e) => {
      if (!hovered || e.pointerType !== 'mouse') return;
      if (e.relatedTarget && hovered.contains(e.relatedTarget)) return;
      const el = hovered; hovered = null; rest(el);
    });
    document.addEventListener('pointerdown', (e) => {
      const el = closest(e);
      if (!el || el.disabled) return;
      pressed = el;
      const s = stateOf(el); s.s.t = 0.9; wake(el);
    });
    const release = () => {
      if (!pressed) return;
      const el = pressed; pressed = null;
      const s = stateOf(el);
      s.s.t = el === hovered ? s.hover : 1;
      s.s.v += 1.4;                                          /* squash then spring back */
      s.z.v += (Math.random() < 0.5 ? -1 : 1) * 30;
      wake(el);
    };
    document.addEventListener('pointerup', release);
    document.addEventListener('pointercancel', release);
  }

  /* ---------- 3. Quiz protection (original behaviour, scoped to the Quiz tab) ---------- */
  window.__quizProtect = false;
  const guarded = (e) => window.__quizProtect && !(e.target && e.target.closest && e.target.closest('input, textarea'));
  ['contextmenu', 'selectstart', 'dragstart', 'copy', 'cut'].forEach(ev =>
    document.addEventListener(ev, (e) => { if (guarded(e)) e.preventDefault(); }, { passive: false }));
  document.addEventListener('keydown', (e) => {
    if (!guarded(e)) return;
    const k = e.key.toLowerCase();
    if ((e.ctrlKey || e.metaKey) && ['c', 'x', 'u', 's', 'a', 'p'].includes(k)) e.preventDefault();
    if ((e.ctrlKey || e.metaKey) && e.shiftKey && ['i', 'c', 'j', '3', '4', '5'].includes(k)) e.preventDefault();
    if (e.key === 'F12' || e.key === 'PrintScreen') e.preventDefault();
  }, { passive: false });
  const setBlur = (on) => {
    const root = document.querySelector('.quiz-root');
    if (!root) return;
    if (on && window.__quizProtect) root.classList.add('blur-protect');
    else setTimeout(() => root.classList.remove('blur-protect'), 100);
  };
  document.addEventListener('visibilitychange', () => setBlur(document.hidden));
  window.addEventListener('blur', () => setBlur(true));
  window.addEventListener('focus', () => setBlur(false));
  document.addEventListener('keyup', (e) => {
    if (e.key === 'PrintScreen' && window.__quizProtect) {
      setBlur(true); navigator.clipboard && navigator.clipboard.writeText('').catch(() => {});
      setTimeout(() => setBlur(false), 1000);
    }
  });
})();
