        const { useState, useEffect, useLayoutEffect, useRef, createElement: h, useMemo, useImperativeHandle, useSyncExternalStore } = React;
        const { motion, AnimatePresence } = window.Motion;

        const EASE = {
            apple:    [0.32, 0.72, 0, 1],
            outExpo:  [0.16, 1, 0.3, 1],
            soft:     [0.4, 0, 0.2, 1],
            glide:    [0.45, 0.05, 0.25, 1],
        };
        const DUR = {
            fast: 0.22,
            med:  0.42,
            slow: 0.6,
            glide: 0.62,
        };
        const FEEDBACK_MS = 300;

        /* ============================================================
           Canvas wave-text engine.
           measureLines: pure-CPU line breaking (unchanged).
           buildLayout:  per-char x positions, measured once.
           paintFrame:   draws the text; with a time value it applies
                         the same 0 → -7px → 0 bob each letter had in
                         the old CSS keyframes.
           ============================================================ */
        let _measureCanvas = null;
        function measureLines(text, maxWidth, cssFont) {
            if (!_measureCanvas) _measureCanvas = document.createElement('canvas');
            const ctx = _measureCanvas.getContext('2d');
            ctx.font = cssFont;
            const tokens = text.split(/(\s+)/);
            const lines = [];
            let cur = '', curW = 0;
            for (const tok of tokens) {
                if (tok === '') continue;
                const tw = ctx.measureText(tok).width;
                if (cur && curW + tw > maxWidth) {
                    lines.push(cur);
                    if (/^\s+$/.test(tok)) { cur = ''; curW = 0; }
                    else { cur = tok; curW = tw; }
                } else { cur += tok; curW += tw; }
            }
            if (cur) lines.push(cur);
            return lines;
        }

        const WAVE_CHAR_STAGGER = 0.022;
        const WAVE_LINE_STAGGER = 0.07;
        const WAVE_DURATION = 1.0;
        const WAVE_AMP = 7;
        const easeInOutQuad = (t) => t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;

        function buildLayout(text, maxWidth, cssFont) {
            if (!_measureCanvas) _measureCanvas = document.createElement('canvas');
            const ctx = _measureCanvas.getContext('2d');
            const lines = measureLines(text, maxWidth, cssFont);
            ctx.font = cssFont;
            return lines.map((line) => {
                const xs = [];
                for (let i = 0; i < line.length; i++) xs.push(ctx.measureText(line.slice(0, i)).width);
                return { text: line, xs };
            });
        }

        function _WaveQuestionCard({ q, i, isActive, opaqueMode, currentQuestion, handleOption,
                                    musicDropdownOpen, calcDropdownOpen, isMobile,
                                    selected, locked, quizStarted }) {
            const containerRef = useRef(null);
            const canvasRef = useRef(null);
            const layoutRef = useRef(null);
            const fontRef = useRef({ cssFont: '16px sans-serif', fontSize: 16, lineHeight: 26, width: 0 });
            const rafRef = useRef(0);
            const [layoutVersion, setLayoutVersion] = useState(0);

            /* Paint one frame. t === null → static (full strings, perfect
               kerning). t === seconds → per-char wave offsets. */
            const paintFrame = (t) => {
                const canvas = canvasRef.current;
                const L = layoutRef.current;
                if (!canvas || !L || !L.length) return;
                const { cssFont, fontSize, lineHeight, width } = fontRef.current;
                const height = L.length * lineHeight + 2;
                const dpr = Math.min(2.5, window.devicePixelRatio || 1);
                const pw = Math.round(width * dpr), ph = Math.round(height * dpr);
                if (canvas.width !== pw || canvas.height !== ph) { canvas.width = pw; canvas.height = ph; }
                canvas.style.width = width + 'px';
                canvas.style.height = height + 'px';
                const ctx = canvas.getContext('2d');
                ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
                ctx.clearRect(0, 0, width, height);
                ctx.font = cssFont;
                ctx.fillStyle = '#FFFFFF';
                ctx.textBaseline = 'alphabetic';
                for (let li = 0; li < L.length; li++) {
                    const line = L[li];
                    const baseY = li * lineHeight + fontSize * 0.92;
                    if (t === null) {
                        ctx.fillText(line.text.replace(/\s+$/, ''), 0, baseY);
                    } else {
                        for (let ci = 0; ci < line.text.length; ci++) {
                            const ch = line.text[ci];
                            if (ch === ' ') continue;
                            const delay = ci * WAVE_CHAR_STAGGER + li * WAVE_LINE_STAGGER;
                            const local = t - delay;
                            let off = 0;
                            if (local > 0 && local < WAVE_DURATION) {
                                const half = WAVE_DURATION / 2;
                                off = local < half
                                    ? -WAVE_AMP * easeInOutQuad(local / half)
                                    : -WAVE_AMP * (1 - easeInOutQuad((local - half) / half));
                            }
                            ctx.fillText(ch, line.xs[ci], baseY + off);
                        }
                    }
                }
            };

            const computeAndSet = () => {
                const el = containerRef.current;
                if (!el) return;
                const cs = getComputedStyle(el);
                const cssFont = `${cs.fontWeight} ${cs.fontSize} ${cs.fontFamily}`;
                const fontSize = parseFloat(cs.fontSize) || 16;
                const lineHeight = fontSize * 1.625; /* leading-relaxed */
                const maxW = el.clientWidth;
                if (maxW <= 0) return;
                fontRef.current = { cssFont, fontSize, lineHeight, width: maxW };
                layoutRef.current = buildLayout(q.question, maxW, cssFont);
                setLayoutVersion(v => v + 1);
            };

            useEffect(() => { computeAndSet(); }, [q.question]);

            useEffect(() => {
                let raf;
                const onResize = () => {
                    cancelAnimationFrame(raf);
                    raf = requestAnimationFrame(() => { cancelAnimationFrame(rafRef.current); computeAndSet(); });
                };
                window.addEventListener('resize', onResize);
                return () => { window.removeEventListener('resize', onResize); cancelAnimationFrame(raf); };
            }, [q.question]);

            /* Re-wrap the canvas text when the card width changes (e.g. a dock panel opens) */
            useEffect(() => {
                const el = containerRef.current;
                if (!el || !window.ResizeObserver) return;
                let raf, lastW = el.clientWidth;
                const ro = new ResizeObserver(() => {
                    if (Math.abs(el.clientWidth - lastW) < 2) return;
                    lastW = el.clientWidth;
                    cancelAnimationFrame(raf);
                    raf = requestAnimationFrame(computeAndSet);
                });
                ro.observe(el);
                return () => { ro.disconnect(); cancelAnimationFrame(raf); };
            }, [q.question]);

            /* Static frame whenever layout changes */
            useEffect(() => { paintFrame(null); }, [layoutVersion]);

            /* Webfonts resolve after first paint — re-measure once ready */
            useEffect(() => {
                let alive = true;
                if (document.fonts && document.fonts.ready) {
                    document.fonts.ready.then(() => { if (alive) computeAndSet(); });
                }
                return () => { alive = false; };
            }, []);

            /* Trigger the wave ~550ms after the card becomes active */
            useEffect(() => {
                if (!isActive || !quizStarted) return;
                const timer = setTimeout(() => {
                    cancelAnimationFrame(rafRef.current);
                    const L = layoutRef.current;
                    if (!L || !L.length) return;
                    let maxDelay = 0;
                    L.forEach((line, li) => { maxDelay = Math.max(maxDelay, line.text.length * WAVE_CHAR_STAGGER + li * WAVE_LINE_STAGGER); });
                    const total = maxDelay + WAVE_DURATION + 0.08;
                    const start = performance.now();
                    const step = (now) => {
                        const tt = (now - start) / 1000;
                        if (tt >= total) { paintFrame(null); return; }
                        paintFrame(Math.max(0, tt));
                        rafRef.current = requestAnimationFrame(step);
                    };
                    rafRef.current = requestAnimationFrame(step);
                }, 380);
                return () => { clearTimeout(timer); cancelAnimationFrame(rafRef.current); };
            }, [isActive, quizStarted, layoutVersion]);

            useEffect(() => () => cancelAnimationFrame(rafRef.current), []);

            const hasImage = !!q.image;
            const isCur  = i === currentQuestion;
            const dropdownOpen = musicDropdownOpen || calcDropdownOpen;
            let targetX = '0%', targetScale = 1, targetOpacity = 0, zIndex = 0;
            if (isCur) {
                targetScale = hasImage ? (dropdownOpen ? 0.94 : (isMobile ? 1 : 1.03)) : (dropdownOpen ? 1 : (isMobile ? 1 : 1.03));
                targetOpacity = 1; zIndex = 20;
            } else if (i === currentQuestion - 1) {
                targetX = '-80%'; targetScale = 0.95; targetOpacity = (dropdownOpen || isMobile) ? 0 : 0.4; zIndex = 10;
            } else if (i === currentQuestion + 1) {
                targetX = '80%'; targetScale = 0.95; targetOpacity = (dropdownOpen || isMobile) ? 0 : 0.4; zIndex = 10;
            } else if (i < currentQuestion) {
                targetX = '-120%'; targetScale = 0.9; zIndex = 5;
            } else {
                targetX = '120%'; targetScale = 0.9; zIndex = 5;
            }
            const cardWidth = hasImage
                ? (isMobile ? 'calc(100vw - 20px)' : 'min(calc(100vw - var(--reserve-x, 0px) - 48px), 980px)')
                : 'min(calc(100vw - var(--reserve-x, 0px) - 28px), 560px)';

            const questionTextNode = h('div', {
                ref: containerRef,
                className: 'mb-6 leading-relaxed question-text-container',
            }, h('canvas', {
                ref: canvasRef,
                role: 'img',
                'aria-label': q.question
            }));

            const optionClass = (idx) => {
                if (!isCur || !locked || selected === null) return 'opt-btn';
                if (idx === selected) return 'opt-btn locked opt-selected';
                return 'opt-btn locked opt-dim';
            };

            const LETTERS = ['A','B','C','D','E','F','G','H','I','J'];
            const renderOptions = () => h('div', { className: 'space-y-2' },
                q.options.map((opt, idx) => h('button', {
                    key: idx,
                    onClick: () => isCur && !locked && handleOption(idx),
                    className: optionClass(idx),
                    disabled: locked,
                    'data-letter': LETTERS[idx] || (idx + 1),
                    'aria-label': `Option ${LETTERS[idx] || (idx+1)}: ${opt}`
                }, opt))
            );

            const totalQuestions = questions.length;
            const progressRail = h('div', { className: 'progress-rail' },
                Array.from({length: totalQuestions}, (_, segIdx) => h('div', {
                    key: segIdx,
                    className: `progress-seg${segIdx < i ? ' done' : segIdx === i ? ' current' : ''}`
                }, h('div', { className: 'progress-seg-fill' })))
            );
            const questionHeader = h('h2', { className: 'text-xl text-[#FF9E7C] font-semibold tracking-tight mb-4', style: { fontFamily: "'Sora', sans-serif" } },
                `Question ${i + 1}`
            );

            const cardInner = hasImage
                ? h('div', { className: 'flex flex-col md:flex-row gap-4 md:gap-6 items-stretch' },
                    h('div', {
                        className: `flex-shrink-0 border border-white/10 p-4 rounded-3xl ${opaqueMode ? 'bg-black/95' : 'bg-black/90'} flex items-center justify-center`,
                        style: { width: isMobile ? '100%' : '40%', minHeight: isMobile ? '200px' : '400px', maxHeight: isMobile ? '30dvh' : 'var(--card-max)' }
                    },
                        h('img', { src: q.image, alt: 'Question illustration', className: 'max-w-full max-h-full object-contain rounded-2xl' })
                    ),
                    h('div', {
                        className: `flex-1 border border-white/10 p-6 rounded-3xl overflow-y-auto ${opaqueMode ? 'bg-black/95' : 'bg-black/90'}`,
                        style: { maxHeight: isMobile ? 'none' : 'var(--card-max)' }
                    },
                        progressRail,
                        questionHeader,
                        questionTextNode,
                        renderOptions()
                    )
                )
                : h('div', {
                    className: `border border-white/10 p-6 rounded-3xl overflow-y-auto max-h-[var(--card-max)] ${opaqueMode ? 'bg-black/95' : 'bg-black/90'}`
                },
                    progressRail,
                    questionHeader,
                    questionTextNode,
                    renderOptions()
                );

            return h(motion.div, {
                className: 'absolute question-card-layer',
                animate: { x: targetX, scale: targetScale, opacity: targetOpacity },
                transition: { duration: 0.5, ease: EASE.glide },
                style: Object.assign({ width: cardWidth, zIndex, willChange: 'transform, opacity' }, isMobile ? { maxHeight: 'var(--card-max)', overflowY: 'auto', borderRadius: '24px' } : {}),
                initial: false
            }, cardInner);
        }
        const WaveQuestionCard = React.memo(_WaveQuestionCard);

        function Calculator({ isDarkMode, isMobile, isOpen, isAnimating, onToggle }) {
            const [disp, setDisp] = useState('0');
            const [prev, setPrev] = useState(null);
            const [op, setOp] = useState(null);
            const [fresh, setFresh] = useState(true);
            const [activeOp, setActiveOp] = useState(null);
            const wrapRef = useRef(null);
            const btnRef = useRef(null);
            const panelRef = useRef(null);

            useEffect(() => {
                const fn = (e) => { if (isOpen && wrapRef.current && !wrapRef.current.contains(e.target)) onToggle(false); };
                document.addEventListener('mousedown', fn);
                return () => document.removeEventListener('mousedown', fn);
            }, [isOpen, onToggle]);

            const toggleOpen = () => onToggle(!isOpen);

            const onTiltMove = (e, ref) => {
                if (!ref.current) return;
                const rect = ref.current.getBoundingClientRect();
                const x = e.clientX - rect.left, y = e.clientY - rect.top;
                const cx = rect.width/2, cy = rect.height/2;
                const rx = ((y-cy)/cy)*-6, ry = ((x-cx)/cx)*6;
                ref.current.style.transform = `perspective(800px) rotateX(${rx}deg) rotateY(${ry}deg) scale3d(1.03,1.03,1.03)`;
            };
            const onTiltLeave = (ref) => {
                if (!ref.current) return;
                ref.current.style.transform = 'perspective(800px) rotateX(0deg) rotateY(0deg) scale3d(1,1,1)';
            };

            const fmt = (n) => { const s = String(n); return s.length > 10 ? parseFloat(n.toPrecision(7)).toString() : s; };
            const compute = (a, b, o) => { if (o === '+') return a + b; if (o === '-') return a - b; if (o === '*') return a * b; if (o === '/') return b !== 0 ? a / b : 0; return b; };
            const pressNum = (n) => { if (fresh) { setDisp(String(n)); setFresh(false); } else setDisp(d => d === '0' ? String(n) : (d.length < 10 ? d + n : d)); };
            const pressDot = () => { if (fresh) { setDisp('0.'); setFresh(false); return; } if (!disp.includes('.')) setDisp(d => d + '.'); };
            const pressOp = (o) => { const cur = parseFloat(disp); if (prev !== null && !fresh) { const res = compute(prev, cur, op); setDisp(fmt(res)); setPrev(res); } else setPrev(cur); setOp(o); setActiveOp(o); setFresh(true); };
            const pressEq = () => { if (op === null || prev === null) return; const res = compute(prev, parseFloat(disp), op); setDisp(fmt(res)); setPrev(null); setOp(null); setActiveOp(null); setFresh(true); };
            const pressClear = () => { setDisp('0'); setPrev(null); setOp(null); setActiveOp(null); setFresh(true); };
            const pressPM = () => setDisp(d => d.startsWith('-') ? d.slice(1) : (d !== '0' ? '-' + d : '0'));
            const pressPct = () => { setDisp(d => fmt(parseFloat(d)/100)); setFresh(true); };
            const Btn = ({ label, type, wide, fn, isAct }) => h('button', { className: `calc-btn calc-btn-${type}${wide?' calc-btn-zero':''}${isAct?' active-op':''}`, onClick: fn, style: wide ? { justifySelf: 'stretch' } : {} }, label);
            const rows = [
                [['C','fn',pressClear],['±','fn',pressPM],['%','fn',pressPct],['÷','op',()=>pressOp('/')]],
                [['7','num',()=>pressNum('7')],['8','num',()=>pressNum('8')],['9','num',()=>pressNum('9')],['×','op',()=>pressOp('*')]],
                [['4','num',()=>pressNum('4')],['5','num',()=>pressNum('5')],['6','num',()=>pressNum('6')],['−','op',()=>pressOp('-')]],
                [['1','num',()=>pressNum('1')],['2','num',()=>pressNum('2')],['3','num',()=>pressNum('3')],['+','op',()=>pressOp('+')]],
                [['0','num',()=>pressNum('0'),true],['.','num',pressDot],['=','op',pressEq]]
            ];
            return h('div',{ref:wrapRef,'data-anchor':'calc',style:{position:'relative',zIndex:9999}},
                h('div',{ref:btnRef,className:`calc-pill ${isOpen?' open':''}${isDarkMode?' dark':''}`,onClick:toggleOpen,'aria-label':'Calculator',title:"Calculator"}, h('span',{className:'calc-icon'},'+')),
                (isOpen || isAnimating) && h('div',{className:`${isMobile?'dock-sheet calc-sheet':'absolute top-full right-0 mt-2'} overflow-visible transition-all duration-150 ${isOpen?'opacity-100 scale-100':'opacity-0 scale-95 pointer-events-none'}`,style:isMobile?{transformOrigin:'bottom center'}:{width:'268px',transformOrigin:'top right',zIndex:9999}},
                    h('div',{ref:panelRef,className:`liquid-glass-calc-panel ${isDarkMode?'dark':''}`,style:{width:'260px',padding:'4px 0'}},
                        h('div',{className:`calc-display${disp.length>9?' small':''}`},disp),
                        h('div',{className:'calc-grid'},
                            rows.map((row,ri)=>row.map(([label,type,fn,wide],ci)=>
                                h(Btn,{key:`${ri}-${ci}`,label,type,wide:!!wide,fn,isAct:(label==='÷'&&activeOp==='/')||(label==='×'&&activeOp==='*')||(label==='−'&&activeOp==='-')||(label==='+'&&activeOp==='+')})
                            ))
                        )
                    )
                )
            );
        }

        const questions = [
            { question: "A 63-year-old man comes to the office due to poor urine flow for several months. The patient says that it takes about 10 seconds to initiate a urinary stream. He also needs to strain when urinating or the stream just stops before his bladder is empty. He has tried behavioral modifications, but the symptoms continue to be bothersome. The patient takes lisinopril for hypertension. He has smoked a pack of cigarettes a day for 30 years. His father had urothelial cancer of the bladder. He reports significant improvement several months after starting finasteride. A therapeutic effect on which of the following structures is most likely responsible for this patient's symptomatic improvement?", options: ["Structure A","Structure B","Structure C","Structure D","Structure E","Structure F"], answer: 2, image: "https://github.com/Bluegates77/Question-Images/blob/main/Question%201.png?raw=true" },
            { question: "A 54-year-old man comes to the office due to difficulty maintaining an erection for the last several weeks. He says, \"One night I was having sex with my wife and could not maintain an erection. Since then, it has continued to be a problem.\" The patient still has morning erections. His medical history is significant for hypertension, coronary artery disease, and depression. He has been married for 20 years but reports that he and his wife have been arguing since he began spending more time at work following a promotion 2 months ago. His current medications include amlodipine, lisinopril, and bupropion. Physical examination is unremarkable. Which of the following is the most likely cause of this patient's erectile dysfunction?", options: ["Advancing age","Antidepressant-induced sexual dysfunction","Antihypertensive-induced adverse effect","Psychological distress","Vascular insufficiency"], answer: 3 },
            { question: "A 21-year-old previously healthy man comes to the emergency department with acute onset of severe left scrotal pain and nausea. The pain started after he returned home from the gym following an intense workout. He denies any trauma, fever, or dysuria. Examination shows a swollen and tender left testis that lies higher than the right testis. Doppler ultrasound shows decreased arterial blood flow in the left spermatic cord entering the testis. Which of the following is the likely origin of the artery involved in this patient's condition?", options: ["Abdominal aorta","External iliac artery","Internal iliac artery","Internal pudendal artery","Obturator artery","Renal artery"], answer: 0 },
            {"question": "A 53-year-old woman comes to the office because of a 6-year history of stiffness and pain of her hands. She has difficulty buttoning her clothes because of the symptoms. She takes no medications. Physical examination shows the findings in the photograph. An abnormality of which of the following is most likely to confirm the diagnosis in this patient?", "options": ["Anti-citrullinated peptide antibody", "Antimitochondrial antibody assay", "Human leukocyte antigen-DQ2 antibody assay", "Precursor of the erythroid cell line", "Precursor of the thrombopoietic line"], "answer": 0, "image": "https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@211e233045daef6509b32705ff4f6025f6c3df20/usmle-quiz/assets/questions/usmle-step1-item02.png"},
            {"question": "An 80-year-old woman is evaluated in a subacute rehabilitation facility 6 months after she sustained a cerebral infarction. Her vital signs are within normal limits. When the patient is asked to look to the right, the right eye abducts and the left eye adducts. When she is asked to look to the left, the right eye adducts and the left eye remains midline. There is right-sided hemiparesis. Deep tendon reflexes are 3+ in the right biceps and quadriceps. Babinski sign is present on the right. Sensation to vibration and two-point discrimination are decreased on the right. The area affected by the cerebral infarction is indicated by the shaded area in the drawing of the brain stem shown. Which of the following additional findings is most likely in this patient?", "options": ["Anesthesia of the left side of the face", "Asymmetric smile", "Hoarseness", "Loss of accommodation", "Paralysis of the tongue"], "answer": 1, "image": "https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@211e233045daef6509b32705ff4f6025f6c3df20/usmle-quiz/assets/questions/usmle-step1-item03.png"},
            {"question": "A 63-year-old woman with a 2-year history of indigestion and heartburn volunteers for a research study in which a variety of drugs are administered and her gastric pH is measured. Which of the following sets of curves best represents the results in this patient after administration of cimetidine and omeprazole?", "options": ["Cimetidine A, omeprazole D", "Cimetidine B, omeprazole A", "Cimetidine B, omeprazole D", "Cimetidine C, omeprazole A", "Cimetidine C, omeprazole D", "Cimetidine D, omeprazole A"], "answer": 1, "image": "https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@211e233045daef6509b32705ff4f6025f6c3df20/usmle-quiz/assets/questions/usmle-step1-item15.png"},
            {"question": "A 65-year-old woman is brought to the emergency department because of a 10-minute history of chest tightness and severe pain of her left arm. Physical examination shows jugular venous distention. Crackles are heard over the lung fields. An ECG shows ST-segment elevation greater than 1 mm in leads V4 through V6 and new Q waves. Serum studies show an increased troponin I concentration. Which of the following labeled points in the graph best represents the changes in cardiac function that occurred during the first 10 seconds after the onset of pain in this patient?", "options": ["W → X", "W → Y", "W → Z", "X → W", "X → Y", "X → Z", "Z → W", "Z → X", "Z → Y"], "answer": 2, "image": "https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@211e233045daef6509b32705ff4f6025f6c3df20/usmle-quiz/assets/questions/usmle-step1-item24.png"},
            {"question": "A 25-year-old man is brought to the emergency department via ambulance 15 minutes after he sustained a gunshot wound to his back during a robbery. On arrival, he is conscious but appears confused. Temperature is 37.0°C (98.6°F), pulse is 120/min and weak, respirations are 20/min, and blood pressure is 110/50 mm Hg. Pulse oximetry on room air shows an oxygen saturation of 96%. Physical examination shows pale, cold, clammy skin and a single bullet entry wound in the posterior 10th intercostal space on the left side. Abdominal CT scan is shown. The bullet is most likely lodged in which of the following structures in this patient?", "options": ["Adrenal gland", "Colon", "Kidney", "Lung", "Spleen"], "answer": 4, "image": "https://cdn.jsdelivr.net/gh/VeeVera123/Web-Crawler@211e233045daef6509b32705ff4f6025f6c3df20/usmle-quiz/assets/questions/usmle-step1-item26.png"}
        ];

        const explanations = [
            { text: `This patient has signs of bladder outlet obstruction (eg, urinary hesitancy, need to strain) most likely due to benign prostatic hyperplasia (BPH), a condition that is present in >90% of men age >80. The diagnosis is based on history of urinary symptoms and digital rectal examination showing a smooth, homogenously enlarged prostate. The prostate is located just anterior to the anal canal (Choice D) and posterior to the symphysis pubis (the above CT image is just superior to the joining of the left and right pubic bones).\n\nBPH may cause static urinary obstruction (androgen-mediated enlargement of the prostate) or dynamic obstruction (prostate smooth muscle contraction via α-adrenoceptors). 5-α reductase inhibitors (eg, finasteride) inhibit the action of androgens on the prostate gland, preventing the conversion of testosterone to dihydrotestosterone and thereby limiting further prostate enlargement. α-adrenergic blockers (eg, tamsulosin) relax the smooth muscle in the bladder neck and prostate gland and are also used to control symptoms of BPH.\n\n(Choice B) The distal end of the anterior bladder wall may or may not be visible at this level depending on patient positioning and bladder filling.\n\nEducational objective:\nThe prostate is located between the pubic symphysis and the anal canal and is visible on inferior sections of the pelvis on CT scan. Benign prostatic hyperplasia is a common, age-related condition that causes urinary symptoms (eg, hesitancy, straining, incomplete voiding). It can be medically treated with α-adrenergic blockers and 5-α reductase inhibitors.`, images: ["https://github.com/Bluegates77/Question-Images/blob/main/Answer%201.jpeg?raw=true","https://github.com/Bluegates77/Question-Images/blob/main/Benign%20prostatic%20hyperplasia%20%28BPH%29.jpeg?raw=true","https://github.com/Bluegates77/Question-Images/blob/main/Benign%20prostatic%20hyperplasia.jpeg?raw=true"] },
            `The temporal association between this patient's arguments with his wife and subsequent onset of erectile dysfunction (ED) makes psychogenic factors the most likely cause of his ED. The presence of spontaneous nocturnal erections is another important diagnostic clue as this demonstrates the integrity of neurologic reflexes and corpus cavernosa blood flow. Loss of normal nocturnal erections occurs in men with organic ED but is not seen with psychogenic etiologies.\n\nED can be caused by organic factors, psychogenic factors, or a combination of both. Psychogenic causes include performance anxiety, depression, sexual trauma, and/or relationship problems and are typically associated with an abrupt onset triggered by stress, as in this patient. In contrast, men suffering from organic causes of ED (eg, vascular insufficiency) tend to have a slower progression of symptoms, with intermittent ED later becoming more persistent in nature (Choice E).\n\n(Choice A) There tends to be an increase in the incidence of ED with advancing age. However, the sudden onset of this patient's dysfunction during a time of relationship and work stress makes a psychogenic cause more likely.\n\n(Choices B and C) Antidepressants, particularly selective serotonin reuptake inhibitors, have been associated with sexual dysfunction. However, the norepinephrine-dopamine reuptake inhibitor bupropion has not been associated with sexual dysfunction. Of the antihypertensive agents, thiazide diuretics and sympathetic blockers (eg, clonidine, methyldopa) have the greatest risk for ED. Angiotensin-converting enzyme inhibitors (eg, lisinopril) and calcium channel blockers (eg, amlodipine) have the least risk.\n\nEducational objective:\nPsychogenic causes of erectile dysfunction include performance anxiety, depression, sexual trauma, relationship problems, and stress. Important clues include sudden-onset and normal nocturnal erections.`,
            `The gonadal arteries arise from the abdominal aorta slightly below the renal arteries. Each gonadal artery courses obliquely downward and laterally within the retroperitoneal space near the psoas major muscle. After crossing anteriorly over the ureter, the gonadal arteries run parallel to the external iliac vessels and eventually traverse the inguinal canal to supply the testes via the spermatic cord in males.\n\nThis patient most likely has testicular torsion, which is usually due to inadequate fixation of the lower pole of the testis to the tunica vaginalis. Testicular torsion is caused by twisting of the spermatic cord, resulting in compression of the pampiniform plexus of the testicular vein and reduced venous outflow. Arterial blood flow in the testicular arteries is initially preserved or slightly decreased, leading to engorgement and eventual hemorrhagic infarction. Torsion is characterized by acute, severe pain with nausea/vomiting, an asymmetrically high-riding testis, and absent cremasteric reflex (elevation of testis while pinching the skin in upper thigh).\n\n(Choices B and C) The external and internal iliac arteries arise from the common iliac artery. The external iliac artery travels underneath the inguinal ligament and becomes the femoral artery, which supplies the lower extremity. The internal iliac artery provides blood supply to the pelvic wall/viscera, buttock, female reproductive organs, bladder, and medial thigh.\n\n(Choice D) The internal pudendal artery is a branch of the internal iliac artery and provides blood supply to the anal canal, scrotum, and penis. However, it does not provide blood supply to the testis.\n\n(Choice E) The obturator artery arises from the internal iliac artery and provides blood supply to the pelvis, bladder, and parts of the femoral head and medial thigh muscles.\n\n(Choice F) The renal artery provides blood supply to the kidney. Branches of the renal artery supply the ureter but do not travel to the testis. Unlike the gonadal arteries, the gonadal veins arise from different structures. The right gonadal vein drains directly into the inferior vena cava while the left gonadal vein drains into the left renal vein.\n\nEducational objective:\nTesticular torsion is due to twisting of the testis around the spermatic cord (containing the gonadal artery), which can eventually lead to ischemia. The gonadal arteries arise from the abdominal aorta. The right gonadal vein drains directly into the inferior vena cava while the left gonadal vein drains into the left renal vein.`,
            {"text": "Official answer: (A) Anti-citrullinated peptide antibody.\n\nThe USMLE publishes its sample questions with an answer key only, so there is no official explanation for this item.\n\nSource: USMLE Step 1 Sample Test Questions (April 2026 update), Item 2. © FSMB / NBME, free public release.", "images": []},
            {"text": "Official answer: (B) Asymmetric smile.\n\nThe USMLE publishes its sample questions with an answer key only, so there is no official explanation for this item.\n\nSource: USMLE Step 1 Sample Test Questions (April 2026 update), Item 3. © FSMB / NBME, free public release.", "images": []},
            {"text": "Official answer: (B) Cimetidine B, omeprazole A.\n\nThe USMLE publishes its sample questions with an answer key only, so there is no official explanation for this item.\n\nSource: USMLE Step 1 Sample Test Questions (April 2026 update), Item 15. © FSMB / NBME, free public release.", "images": []},
            {"text": "Official answer: (C) W → Z.\n\nThe USMLE publishes its sample questions with an answer key only, so there is no official explanation for this item.\n\nSource: USMLE Step 1 Sample Test Questions (April 2026 update), Item 24. © FSMB / NBME, free public release.", "images": []},
            {"text": "Official answer: (E) Spleen.\n\nThe USMLE publishes its sample questions with an answer key only, so there is no official explanation for this item.\n\nSource: USMLE Step 1 Sample Test Questions (April 2026 update), Item 26. © FSMB / NBME, free public release.", "images": []}
        ];

        const firstRowImages = ["https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images@main/Download%202.jpg","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@main/Black%20Car.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@main/Brunch.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@main/Mirror.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@main/book.jfif"];
        const secondRowImages = ["https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@2nd-row/Asthetic%20Polo.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@2nd-row/2000s%20fashion.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@2nd-row/Aesthetic%20Cafe%20Vibe.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@2nd-row/Studio%20setup%20_%20Oakywood%20Classic%20Desk.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@2nd-row/Coffe.jfif"];
        const thirdRowImages = ["https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Third-row/KayandTay.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Third-row/%40__nigrum.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Third-row/Black%20friends%20(2).jpg","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Third-row/Chicken%20.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Third-row/fdoc!.jfif"];
        const fourthRowImages = ["https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Fourth-row/Model.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Fourth-row/IMG_6225.JPG","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Fourth-row/%40sophiacuerquis.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Fourth-row/Khaki_Brown_Blue.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Fourth-row/rich%20life.jfif"];
        const fifthRowImages = ["https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@5th-row/Call%20me%20daddy%20lol.jpg","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@5th-row/Blob.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@5th-row/Fucking%20Aura.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@5th-row/Thongs.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@5th-row/Legs%20in%20the%20air%20.jpg"];
        const sixthRowImages = ["https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@6th-row/Black%20and%20fucking%20beautiful.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@6th-row/Crossed%20legs%20(2).jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@6th-row/Guitar.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@6th-row/Plus-size%20Butter%20lemon%20Yellow%20Oversize%20Outfit%20Men%20Women%20Casual%20CompfyFall%20Ideas.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@6th-row/Dread.jfif"];
        const seventhRowImages = ["https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@7th-row/Jeans%20and%20cups.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@7th-row/Multicolored%20car.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@7th-row/Sunflowers%20.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@7th-row/Sunset.jfif","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@7th-row/Spiddey.jfif"];
        const eighthRowImages = ["https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@8th-row/Hands.JPG","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@8th-row/Girls.JPG","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@8th-row/Go%20girl.JPG","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@8th-row/Green%20car.JPG","https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@8th-row/Haha.JPG"];

        const PLAYLISTS = [
            { id:"im-not-okay", name:"I'm <not> Okay", cover:"https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@Fourth-row/IMG_6225.JPG", songs:[{title:"Butterflies - Tom Odell",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/I'm-%3CNot%3E-Okay/Butterflies%20-%20Tom%20Odell.mp4"},{title:"Friends - Aubory",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/I'm-%3CNot%3E-Okay/Friends%20-Aubory.mp3"},{title:"Supermarket Flowers - Ed Sheeran",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/I'm-%3CNot%3E-Okay/Supermarket%20Flowers%20-%20Ed%20Sheeran%20.mp4"},{title:"Still Mine - Ashley Sighn",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/I'm-%3CNot%3E-Okay/Still%20Mine%20-%20Ashley%20Sighn.mp4"},{title:"Uncalled For - Emily James",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/I'm-%3CNot%3E-Okay/Uncalled%20For%20-%20Emily%20James%20.mp4"}] },
            { id:"night-drive", name:"Night Drive", cover:"https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@7th-row/Sunflowers%20.jfif", songs:[{title:"All Girls Are The Same - Juice",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Night-Drive/All%20Girls%20Are%20The%20Same%20-%20Juice%20.mp3"},{title:"Dear God - Dax",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Night-Drive/Dear%20God%20-%20Dax.mp3"},{title:"Fast - Juice Wrld",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Night-Drive/Fast%20-%20Juice%20Wrld.mp3"},{title:"Let You Down - NF",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Night-Drive/Let%20You%20Down%20-%20NF.mp3"},{title:"Lie - NF",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Night-Drive/Lie%20-%20NF.mp3"}] },
            { id:"trybe", name:"Trybe", cover:"https://cdn.jsdelivr.net/gh/Bluegates77/Quiz-Images-II@7th-row/Spiddey.jfif", songs:[{title:"Die Hard - Kendrick Lamar",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Trybe/Die%20Hard%20-%20Kendrick%20Lamar%20.mp3"},{title:"Hotline Bling - Drake",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Trybe/Hotline%20Bling%20-%20Drake.mp3"},{title:"Passionfruit - Drake",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Trybe/Passionfruit%20-%20%20Drake.mp3"},{title:"Pretty Little Fears - 6LACK ft J Cole",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Trybe/Pretty%20Little%20Fears%20-%206LACK%20ft%20J%20Cole.mp3"},{title:"Tranquility - Jack Harlow",url:"https://github.com/Bluegates77/Songs/raw/refs/heads/Trybe/Tranquility%20-%20Jack%20Harlow.mp3"}] }
        ];
        const DEFAULT_SONG_URL = PLAYLISTS[0]?.songs[0]?.url || "";

        const _FloatingBg = function FloatingBackground() {
            const ANIMATION_DURATION = 50;
            const ROW_DELAY = ANIMATION_DURATION / 8;
            const IMAGE_STAGGER = 0.05;
            const isMobile = window.innerWidth < 768;
            const imageWidth  = isMobile ? '17vw'   : '16vw';
            const imageHeight = isMobile ? '400px'  : '460px';
            const imageGap    = isMobile ? '1.5vw'  : '2vw';
            const rows = [
                {images:firstRowImages,rowIndex:0},{images:secondRowImages,rowIndex:1},
                {images:thirdRowImages,rowIndex:2},{images:fourthRowImages,rowIndex:3},
                {images:fifthRowImages,rowIndex:4},{images:sixthRowImages,rowIndex:5},
                {images:seventhRowImages,rowIndex:6},{images:eighthRowImages,rowIndex:7}
            ];
            return h('div', { className: "absolute inset-0 z-0 overflow-hidden floating-bg-container" },
                rows.slice(0, isMobile ? 4 : 8).map(row => {
                    const images = row.images;
                    const num = images.length;
                    const totalRowWidth = num * parseFloat(imageWidth) + (num - 1) * parseFloat(imageGap);
                    const offset = (100 - totalRowWidth) / 2;
                    return images.map((url, i) => {
                        const delay = row.rowIndex * ROW_DELAY + i * IMAGE_STAGGER;
                        const left = offset + i * (parseFloat(imageWidth) + parseFloat(imageGap));
                        return h(motion.img, {
                            key: `${row.rowIndex}-${i}`,
                            src: url,
                            className: "absolute object-cover rounded-3xl shadow-xl",
                            style: { width: imageWidth, height: imageHeight, left: `${left}vw`, willChange: 'transform' },
                            initial:  { y: "-620px" },
                            animate:  { y: "calc(100vh + 2900px)" },
                            transition: { duration: ANIMATION_DURATION, ease: "linear", delay, repeat: Infinity }
                        });
                    });
                })
            );
        };
        const FloatingBackground = React.memo(_FloatingBg, () => true);

        const Timer = React.forwardRef(function Timer({ isDarkMode, running, duration, onExpire }, ref) {
            const [display, setDisplay] = useState(duration);
            const remainingRef = useRef(duration);
            const onExpireRef = useRef(onExpire);
            onExpireRef.current = onExpire;

            useImperativeHandle(ref, () => ({
                getTimeLeft: () => remainingRef.current,
                reset: () => { remainingRef.current = duration; setDisplay(duration); }
            }), [duration]);

            useEffect(() => {
                if (!running) return;
                const interval = setInterval(() => {
                    remainingRef.current = Math.max(0, remainingRef.current - 1);
                    setDisplay(remainingRef.current);
                    if (remainingRef.current <= 0) {
                        clearInterval(interval);
                        onExpireRef.current && onExpireRef.current();
                    }
                }, 1000);
                return () => clearInterval(interval);
            }, [running]);

            const formatTime = s => `${String(Math.floor(s / 60)).padStart(2,'0')}:${String(s % 60).padStart(2,'0')}`;

            return h('div', { className: `liquid-glass-timer ${isDarkMode?'dark':''} absolute z-30 px-4 md:px-6 py-2 md:py-3 rounded-2xl font-mono font-bold text-base md:text-lg`, style: { top: '1rem', left: 'var(--edge, 6vw)', height: '48px', display: 'flex', alignItems: 'center' } },
                h('span', { className: `liquid-glass-content ${isDarkMode?'text-white/90':'text-gray-800'}` }, formatTime(display))
            );
        });

        function AnimatedNumber({ value, suffix = '', duration = 900 }) {
            const [display, setDisplay] = useState(0);
            useEffect(() => {
                const start = performance.now();
                let raf;
                const target = Number(value) || 0;
                const tick = (now) => {
                    const t = Math.min(1, (now - start) / duration);
                    const eased = 1 - Math.pow(1 - t, 4);
                    setDisplay(Math.round(target * eased));
                    if (t < 1) raf = requestAnimationFrame(tick);
                };
                raf = requestAnimationFrame(tick);
                return () => cancelAnimationFrame(raf);
            }, [value, duration]);
            return h(React.Fragment, null, display, suffix);
        }

        const formatMS = (secs) => {
            const s = Math.max(0, Math.round(secs || 0));
            return `${String(Math.floor(s/60)).padStart(2,'0')}:${String(s%60).padStart(2,'0')}`;
        };

        function AnimatedTime({ seconds, duration = 1100 }) {
            const [v, setV] = useState(0);
            const target = Math.max(0, Math.round(Number(seconds) || 0));
            useEffect(() => {
                if (target === 0) { setV(0); return; }
                const start = performance.now();
                let raf;
                const tick = (now) => {
                    const t = Math.min(1, (now - start) / duration);
                    const eased = 1 - Math.pow(1 - t, 4);
                    setV(Math.round(target * eased));
                    if (t < 1) raf = requestAnimationFrame(tick);
                };
                raf = requestAnimationFrame(tick);
                return () => cancelAnimationFrame(raf);
            }, [target, duration]);
            return h(React.Fragment, null, formatMS(v));
        }

        /* Pop-in delta pill (spring) */
        function PopPill({ text, tone = 'up', delay = 1 }) {
            return h(motion.span, {
                className: `nexus-pill ${tone === 'up' ? 'nexus-pill-up' : 'nexus-pill-down'}`,
                initial: { scale: 0.4, opacity: 0 },
                animate: { scale: 1, opacity: 1 },
                transition: { type: 'spring', stiffness: 520, damping: 26, delay }
            }, text);
        }

        /* Animated SVG progress ring for the score card */
        function ScoreRing({ percent, delay = 0.45, size = 118 }) {
            const stroke = 9;
            const R = (size - stroke) / 2 - 2;
            const C = 2 * Math.PI * R;
            const c = size / 2;
            const clamped = Math.min(100, Math.max(0, percent));
            return h('div', { className: 'relative flex-shrink-0', style: { width: size, height: size } },
                h('svg', { width: size, height: size, viewBox: `0 0 ${size} ${size}`, style: { transform: 'rotate(-90deg)', overflow: 'visible' } },
                    h('defs', null,
                        h('linearGradient', { id: 'scoreRingGrad', x1: '0%', y1: '0%', x2: '100%', y2: '100%' },
                            h('stop', { offset: '0%', stopColor: '#FF9E7C' }),
                            h('stop', { offset: '100%', stopColor: '#FFD3B8' })
                        )
                    ),
                    h('circle', { cx: c, cy: c, r: R, fill: 'none', stroke: 'rgba(255,255,255,0.07)', strokeWidth: stroke }),
                    h(motion.circle, {
                        cx: c, cy: c, r: R, fill: 'none',
                        stroke: 'url(#scoreRingGrad)',
                        strokeWidth: stroke, strokeLinecap: 'round',
                        strokeDasharray: C,
                        initial: { strokeDashoffset: C },
                        animate: { strokeDashoffset: C * (1 - clamped / 100) },
                        transition: { duration: 1.5, delay, ease: [0.16, 1, 0.3, 1] }
                    })
                ),
                h('div', { className: 'absolute inset-0 flex flex-col items-center justify-center' },
                    h('span', { className: 'font-grotesk text-white font-bold leading-none', style: { fontSize: size * 0.24, letterSpacing: '-0.02em' } },
                        h(AnimatedNumber, { value: percent, suffix: '%' })),
                    h('span', { className: 'qcard-label mt-1', style: { fontSize: 9 } }, 'Score')
                )
            );
        }

        /* Card shell: staggered entrance + spring hover lift */
        function ResultCard({ delay = 0, className = '', style, children }) {
            return h(motion.div, {
                className: `qcard ${className}`,
                style,
                initial: 'hidden',
                animate: 'show',
                whileHover: 'hover',
                variants: {
                    hidden: { opacity: 0, y: 30 },
                    show: { opacity: 1, y: 0, transition: { duration: 0.75, delay, ease: EASE.outExpo } },
                    hover: { y: -5, transition: { type: 'spring', stiffness: 400, damping: 24 } }
                }
            }, children);
        }

        /* Mini bar sparkline of recent attempts */
        function AttemptBars({ history, delay = 0.7 }) {
            const items = history.slice(-6);
            if (!items.length) return null;
            return h('div', { className: 'flex items-end gap-1.5', style: { height: 34 } },
                items.map((a, idx) => {
                    const pct = Math.round((a.score / a.total) * 100);
                    const isLast = idx === items.length - 1;
                    return h(motion.div, {
                        key: a.timestamp + '-' + idx,
                        title: `${pct}%`,
                        className: 'rounded-full',
                        style: {
                            width: 7,
                            height: `${Math.max(22, pct)}%`,
                            background: isLast ? 'linear-gradient(180deg,#FFB899,#FF9E7C)' : 'rgba(255,255,255,0.14)',
                            transformOrigin: 'bottom'
                        },
                        initial: { scaleY: 0 },
                        animate: { scaleY: 1 },
                        transition: { duration: 0.7, delay: delay + idx * 0.07, ease: EASE.outExpo }
                    });
                })
            );
        }

        function ResultsChart({ scores }) {
            const canvasRef = useRef(null);
            const chartRef  = useRef(null);
            useEffect(() => {
                if (!canvasRef.current || typeof Chart === 'undefined') return;
                const ctx = canvasRef.current.getContext('2d');
                const grad = ctx.createLinearGradient(0, 0, 0, 320);
                grad.addColorStop(0, 'rgba(255,158,124,0.22)');
                grad.addColorStop(1, 'rgba(255,158,124,0)');
                const labels = scores.map((_, i) => `#${i+1}`);
                chartRef.current = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels,
                        datasets: [{
                            data: scores,
                            borderColor: '#FF9E7C',
                            borderWidth: 2,
                            backgroundColor: grad,
                            fill: true,
                            tension: 0.3,
                            pointRadius: scores.length > 1 ? 3 : 0,
                            pointBackgroundColor: '#FF9E7C',
                            pointBorderColor: 'rgba(0,0,0,0.4)',
                            pointHoverRadius: 6,
                            pointHoverBackgroundColor: '#FFF',
                            pointHoverBorderColor: '#FF9E7C',
                            pointHoverBorderWidth: 2
                        }]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        animation: { duration: 800, easing: 'easeOutQuart' },
                        plugins: {
                            legend: { display: false },
                            tooltip: {
                                backgroundColor: '#151110',
                                titleColor: '#8b8b8b',
                                bodyColor: '#FFF',
                                titleFont: { size: 12, family: 'Manrope' },
                                bodyFont:  { size: 14, family: 'Space Grotesk', weight: '600' },
                                padding: 12,
                                borderColor: 'rgba(255,158,124,0.25)',
                                borderWidth: 1,
                                displayColors: false,
                                callbacks: { label: (c) => `${c.raw}%` }
                            }
                        },
                        scales: {
                            x: { grid: { display: false }, border: { display: false },
                                 ticks: { color: '#8b8b8b', font: { family: 'Manrope', size: 11 } } },
                            y: { grid: { color: 'rgba(255,255,255,0.05)', drawBorder: false },
                                 border: { display: false },
                                 ticks: { callback: (v) => `${v}%`, color: '#8b8b8b',
                                          font: { family: 'Manrope', size: 11 } },
                                 min: 0, max: 100 }
                        },
                        interaction: { intersect: false, mode: 'index' }
                    }
                });
                return () => { if (chartRef.current) { chartRef.current.destroy(); chartRef.current = null; } };
            }, [JSON.stringify(scores)]);
            return h('canvas', { ref: canvasRef, style: { width: '100%', height: '100%' } });
        }

        /* Performance History panel — the All / Last 5 tabs now actually filter */
        function HistoryPanel({ scores, delay = 0.4 }) {
            const [range, setRange] = useState('all');
            const data = range === 'all' ? scores : scores.slice(-5);
            return h(ResultCard, { delay, className: 'lg:col-span-2 p-6 flex flex-col', style: { height: 400 } },
                h('div', { className: 'flex justify-between items-center mb-6' },
                    h('div', null,
                        h('div', { className: 'qcard-label mb-1' }, 'Analytics'),
                        h('span', { className: 'text-sm font-semibold text-white' }, 'Performance History')
                    ),
                    h('div', { className: 'flex gap-1.5' },
                        [['all', 'All'], ['last', 'Last 5']].map(([val, label]) =>
                            h('button', {
                                key: val,
                                onClick: () => setRange(val),
                                className: `nexus-tab ${range === val ? 'active' : ''}`
                            }, label)
                        )
                    )
                ),
                h('div', { className: 'flex-1 relative w-full', style: { minHeight: 0 } },
                    h(ResultsChart, { scores: data })
                )
            );
        }

        function PhysiologyQuiz({ active = true }) {
            const activeRef = useRef(active); activeRef.current = active;
            const [quizStarted, setQuizStarted] = useState(false);
            const [currentQuestion, setCurrentQuestion] = useState(0);
            const [selected, setSelected] = useState(null);
            const [locked, setLocked] = useState(false);
            const [score, setScore] = useState(0);
            const [completed, setCompleted] = useState(false);
            const [timeIsUp, setTimeIsUp] = useState(false);
            const timerRef = useRef(null);
            const [isMobile, setIsMobile] = useState(() => window.innerWidth < 768);
            const [isDarkMode, setIsDarkMode] = useState(() => !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches));
            const [opaqueMode, setOpaqueMode] = useState(false);
            const [reviewIndex, setReviewIndex] = useState(0);
            const [showReview, setShowReview] = useState(false);
            const [lightboxOpen, setLightboxOpen] = useState(false);
            const [lightboxImageIndex, setLightboxImageIndex] = useState(0);
            const [correctQuestions, setCorrectQuestions] = useState([]);
            const quizHistory = useSyncExternalStore(window.AttemptStore.subscribe, window.AttemptStore.get);
            const [totalTimeUsed, setTotalTimeUsed] = useState(0);
            const [typingText, setTypingText] = useState('');
            const [musicDropdownOpen, setMusicDropdownOpen] = useState(false);
            const [calcDropdownOpen, setCalcDropdownOpen] = useState(false);
            const [dropdownAnimating, setDropdownAnimating] = useState(false);
            const [calcDropdownAnimating, setCalcDropdownAnimating] = useState(false);
            const [currentPlaylist, setCurrentPlaylist] = useState(PLAYLISTS[0]);
            const [currentSongIndex, setCurrentSongIndex] = useState(0);
            const [isPlaying, setIsPlaying] = useState(false);
            const [audioStarted, setAudioStarted] = useState(false);
            const audioRef = useRef(null);
            const dropdownRef = useRef(null);
            const carouselRef = useRef(null);
            const musicBtnRef = useRef(null);
            const timeoutRef = useRef(null);

            useEffect(() => {
                const explanationImgs = [];
                explanations.forEach(e => {
                    if (e && typeof e === 'object' && Array.isArray(e.images)) e.images.forEach(u => explanationImgs.push(u));
                });
                const allUrls = [
                    ...questions.map(q2 => q2.image).filter(Boolean),
                    ...explanationImgs,
                    ...firstRowImages, ...secondRowImages, ...thirdRowImages, ...fourthRowImages,
                    ...fifthRowImages, ...sixthRowImages, ...seventhRowImages, ...eighthRowImages,
                    ...PLAYLISTS.map(p => p.cover)
                ];
                allUrls.forEach(url => {
                    const img = new Image();
                    img.src = url;
                    if (img.decode) img.decode().catch(() => {});
                    if (typeof fetch !== 'undefined') {
                        fetch(url, { mode: 'no-cors', cache: 'force-cache' }).catch(() => {});
                    }
                });
            }, []);

            useEffect(() => {
                if (quizStarted) return;
                const sequence = [
                    { type: 'type', text: 'Hi', speed: 150 },
                    { type: 'pause', duration: 700 },
                    { type: 'delete', speed: 80 },
                    { type: 'pause', duration: 400 },
                    { type: 'type', text: 'Fuck this', speed: 120 },
                    { type: 'pause', duration: 1000 },
                    { type: 'delete', speed: 70 },
                    { type: 'pause', duration: 400 },
                    { type: 'type', text: 'Hello world...', speed: 130 },
                    { type: 'pause', duration: 1200 },
                    { type: 'start' }
                ];
                let index = 0, currentText = '', charIndex = 0, cancelled = false;
                const run = () => {
                    if (cancelled || index >= sequence.length) return;
                    const step = sequence[index];
                    if (step.type === 'type') {
                        if (charIndex < step.text.length) {
                            currentText = step.text.substring(0, charIndex + 1);
                            setTypingText(currentText); charIndex++;
                            timeoutRef.current = setTimeout(run, step.speed);
                        } else { charIndex = 0; index++; timeoutRef.current = setTimeout(run, 200); }
                    } else if (step.type === 'delete') {
                        if (currentText.length > 0) { currentText = currentText.slice(0, -1); setTypingText(currentText); timeoutRef.current = setTimeout(run, step.speed); }
                        else { index++; timeoutRef.current = setTimeout(run, 200); }
                    } else if (step.type === 'pause') { index++; timeoutRef.current = setTimeout(run, step.duration); }
                    else if (step.type === 'start') { setQuizStarted(true); }
                };
                timeoutRef.current = setTimeout(run, 300);
                return () => { cancelled = true; clearTimeout(timeoutRef.current); };
            }, [quizStarted]);

            const onTiltMove = (e, ref) => {
                if (!ref.current) return;
                const rect = ref.current.getBoundingClientRect();
                const x = e.clientX - rect.left, y = e.clientY - rect.top;
                const cx = rect.width/2, cy = rect.height/2;
                const rx = ((y-cy)/cy)*-5, ry = ((x-cx)/cx)*5;
                ref.current.style.transform = `perspective(800px) rotateX(${rx}deg) rotateY(${ry}deg) scale3d(1.02,1.02,1.02)`;
            };
            const onTiltLeave = (ref) => {
                if (!ref.current) return;
                ref.current.style.transform = 'perspective(800px) rotateX(0deg) rotateY(0deg) scale3d(1,1,1)';
            };

            /* Audio. Refs keep the handlers free of stale state (the original "ended" handler always
               advanced the FIRST playlist). Adds seek + previous/next. */
            const playlistRef = useRef(currentPlaylist); playlistRef.current = currentPlaylist;
            const songIndexRef = useRef(currentSongIndex); songIndexRef.current = currentSongIndex;
            const [audioProgress, setAudioProgress] = useState({ cur: 0, dur: 0 });
            const [audioError, setAudioError] = useState(false);
            const wantPlayRef = useRef(false);   /* only report load errors the user actually triggered */

            const goRelative = (dir) => {
                const pl = playlistRef.current;
                const ni = (songIndexRef.current + dir + pl.songs.length) % pl.songs.length;
                setCurrentSongIndex(ni); songIndexRef.current = ni;
                setAudioError(false); setAudioProgress({ cur: 0, dur: 0 });
                const a = audioRef.current; wantPlayRef.current = true;
                if (a) { a.src = pl.songs[ni].url; a.play().then(() => { setIsPlaying(true); setAudioStarted(true); }).catch(() => {}); }
            };
            const goRef = useRef(goRelative); goRef.current = goRelative;

            useEffect(() => {
                if (!DEFAULT_SONG_URL) return;
                const audio = new Audio(DEFAULT_SONG_URL);
                audio.loop = false; audio.volume = 0.3; audio.preload = 'none';
                audioRef.current = audio;
                const sync = () => setAudioProgress({ cur: audio.currentTime || 0, dur: isFinite(audio.duration) ? audio.duration : 0 });
                const on = {
                    ended: () => goRef.current(1),
                    play: () => setIsPlaying(true),
                    pause: () => setIsPlaying(false),
                    timeupdate: sync, loadedmetadata: sync,
                    error: () => { if (wantPlayRef.current) setAudioError(true); setIsPlaying(false); }
                };
                Object.entries(on).forEach(([n, f]) => audio.addEventListener(n, f));
                return () => {
                    Object.entries(on).forEach(([n, f]) => audio.removeEventListener(n, f));
                    audio.pause(); audioRef.current = null;
                };
            }, []);

            /* Music starts on the first interaction inside the quiz (as in the original). */
            useEffect(() => {
                if (!DEFAULT_SONG_URL) return;
                const startAudio = () => {
                    if (!activeRef.current) return;
                    if (audioRef.current && !audioStarted) {
                        audioRef.current.play().then(() => { setAudioStarted(true); setIsPlaying(true); }).catch(() => {});
                    }
                };
                document.addEventListener('click', startAudio);
                document.addEventListener('keydown', startAudio);
                return () => {
                    document.removeEventListener('click', startAudio);
                    document.removeEventListener('keydown', startAudio);
                };
            }, [audioStarted]);

            const playSong = (playlist, songIndex) => {
                setCurrentPlaylist(playlist); setCurrentSongIndex(songIndex);
                playlistRef.current = playlist; songIndexRef.current = songIndex;
                setAudioError(false); setAudioProgress({ cur: 0, dur: 0 }); wantPlayRef.current = true;
                if (audioRef.current) {
                    audioRef.current.src = playlist.songs[songIndex].url;
                    audioRef.current.play().then(() => { setIsPlaying(true); setAudioStarted(true); }).catch(() => {});
                }
            };
            const togglePlayPause = (e) => {
                if (e) e.stopPropagation();
                wantPlayRef.current = true;
                if (audioRef.current) {
                    if (isPlaying) { audioRef.current.pause(); setIsPlaying(false); }
                    else { audioRef.current.play().then(() => setIsPlaying(true)).catch(() => setAudioError(true)); }
                }
            };
            const seekTo = (t) => { if (audioRef.current && isFinite(t)) audioRef.current.currentTime = t; };

            /* Dock: reserve room beside / below the content for whichever panel is open so the
               question, results and review cards shift away instead of sitting underneath it. */
            const rootRef = useRef(null);
            const [reserve, setReserve] = useState({ x: 0, y: 0 });
            const [viewportTick, setViewportTick] = useState(0);
            useEffect(() => {
                const on = () => setViewportTick(t => t + 1);
                window.addEventListener('resize', on);
                return () => window.removeEventListener('resize', on);
            }, []);
            useLayoutEffect(() => {
                const open = musicDropdownOpen ? 'music' : calcDropdownOpen ? 'calc' : null;
                const root = rootRef.current;
                if (!root || !open) { setReserve(r => (r.x || r.y) ? { x: 0, y: 0 } : r); return; }
                const vw = root.clientWidth, vh = root.clientHeight;
                if (isMobile) { setReserve({ x: 0, y: open === 'music' ? Math.round(Math.min(vh * 0.52, 440)) + 8 : 400 }); return; }
                const anchor = root.querySelector(`[data-anchor="${open}"]`);
                if (!anchor) return;
                const right = anchor.getBoundingClientRect().right - root.getBoundingClientRect().left;
                const panelW = open === 'music' ? Math.min(420, vw * 0.9) : 268;
                setReserve({ x: Math.max(0, Math.round(vw - (right - panelW) + 16)), y: 0 });
            }, [musicDropdownOpen, calcDropdownOpen, isMobile, viewportTick]);
            /* Let the site shell know a dock panel is open (it hides the mobile tab bar behind the sheet). */
            useEffect(() => {
                document.body.dataset.dock = (musicDropdownOpen || calcDropdownOpen) ? 'open' : '';
                return () => { document.body.dataset.dock = ''; };
            }, [musicDropdownOpen, calcDropdownOpen]);

            const toggleMusicDropdown = () => {
                if (dropdownAnimating || calcDropdownAnimating) return;
                setDropdownAnimating(true);
                if (musicDropdownOpen) { setTimeout(() => { setMusicDropdownOpen(false); setDropdownAnimating(false); }, 150); }
                else {
                    if (calcDropdownOpen) { setCalcDropdownOpen(false); setCalcDropdownAnimating(false); }
                    setMusicDropdownOpen(true);
                    setTimeout(() => setDropdownAnimating(false), 50);
                }
            };

            const handleCalcToggle = (nextState) => {
                if (dropdownAnimating || calcDropdownAnimating) return;
                setCalcDropdownAnimating(true);
                if (nextState) {
                    if (musicDropdownOpen) { setMusicDropdownOpen(false); setDropdownAnimating(false); }
                    setCalcDropdownOpen(true);
                    setTimeout(() => setCalcDropdownAnimating(false), 50);
                } else {
                    setTimeout(() => { setCalcDropdownOpen(false); setCalcDropdownAnimating(false); }, 150);
                }
            };

            useEffect(() => {
                const mq = window.matchMedia('(prefers-color-scheme: dark)');
                const hc = (e) => setIsDarkMode(e.matches);
                mq.addEventListener('change', hc);
                return () => mq.removeEventListener('change', hc);
            }, []);

            const toggleDarkMode = () => setIsDarkMode(prev => !prev);
            const toggleOpaqueMode = () => setOpaqueMode(prev => !prev);

            useEffect(() => {
                let frame;
                const onResize = () => {
                    cancelAnimationFrame(frame);
                    frame = requestAnimationFrame(() => setIsMobile(window.innerWidth < 768));
                };
                window.addEventListener('resize', onResize);
                return () => { window.removeEventListener('resize', onResize); cancelAnimationFrame(frame); };
            }, []);

            const handleTimeExpire = () => {
                setTotalTimeUsed(1800);
                setTimeIsUp(true);
                setCompleted(true);
            };

            const handleOptionRef = useRef();
            handleOptionRef.current = (i) => {
                if (locked || selected !== null) return;
                setSelected(i);
                setLocked(true);
                if (i === questions[currentQuestion].answer) {
                    setScore(s => s + 1);
                    setCorrectQuestions(prev => [...prev, currentQuestion]);
                }
                setTimeout(() => {
                    if (currentQuestion + 1 < questions.length) {
                        setCurrentQuestion(q => q + 1);
                        setSelected(null);
                        setLocked(false);
                    } else {
                        setTotalTimeUsed(1800 - (timerRef.current ? timerRef.current.getTimeLeft() : 0));
                        setCompleted(true);
                    }
                }, FEEDBACK_MS);
            };
            const handleOption = useRef((i) => handleOptionRef.current(i)).current;

            useEffect(() => {
                const onKey = (e) => {
                    if (!activeRef.current) return;
                    if (e.target && (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA')) return;
                    if (musicDropdownOpen || calcDropdownOpen || lightboxOpen) return;
                    if (!quizStarted) {
                        if (e.key === ' ' || e.key === 'Enter') {
                            e.preventDefault();
                            clearTimeout(timeoutRef.current);
                            setQuizStarted(true);
                        }
                        return;
                    }
                    if (completed) {
                        if (e.key === 'r' || e.key === 'R') { e.preventDefault(); handleRewriteQuiz(); }
                        return;
                    }
                    const numMatch = e.key.match(/^[1-9]$/);
                    if (numMatch) {
                        const idx = parseInt(e.key, 10) - 1;
                        const opts = questions[currentQuestion]?.options || [];
                        if (idx >= 0 && idx < opts.length && !locked) {
                            e.preventDefault();
                            handleOption(idx);
                        }
                    }
                };
                window.addEventListener('keydown', onKey);
                return () => window.removeEventListener('keydown', onKey);
            }, [quizStarted, completed, currentQuestion, locked, musicDropdownOpen, calcDropdownOpen, lightboxOpen]);

            const getExplanationData = (exp) => { if (exp === null || exp === undefined) return null; if (typeof exp === 'string') return { text: exp, images: [] }; return { text: exp.text, images: exp.images || [] }; };
            const questionsWithExplanations = questions.map((q, i) => { const ed = getExplanationData(explanations[i]); if (!ed) return null; return { ...q, index: i, explanationText: ed.text, explanationImages: ed.images }; }).filter(Boolean);

            const handlePreviousReview = () => { if (reviewIndex > 0) setReviewIndex(reviewIndex - 1); };
            const handleNextReview = () => { if (reviewIndex < questionsWithExplanations.length - 1) setReviewIndex(reviewIndex + 1); };
            const handleStartReview = () => { setShowReview(true); setReviewIndex(0); };
            const handleBackToResults = () => setShowReview(false);

            useLayoutEffect(() => {
                if (completed && !showReview) {
                    window.AttemptStore.add({ score, total: questions.length, timeUsed: totalTimeUsed, timestamp: Date.now(), correct: correctQuestions, timeUp: timeIsUp });
                }
            }, [completed]);

            useEffect(() => {
                const el = carouselRef.current; if (!el || !lightboxOpen) return;
                let startX = 0, isDragging = false;
                const onTouchStart = (e) => { startX = e.touches[0].clientX; };
                const onTouchEnd = (e) => {
                    const dx = e.changedTouches[0].clientX - startX;
                    if (Math.abs(dx) > 40) {
                        if (dx < 0) setLightboxImageIndex(prev => Math.min((questionsWithExplanations[reviewIndex]?.explanationImages?.length || 1) - 1, prev + 1));
                        else setLightboxImageIndex(prev => Math.max(0, prev - 1));
                    }
                };
                const onMouseDown = (e) => { startX = e.clientX; isDragging = true; };
                const onMouseUp = (e) => {
                    if (!isDragging) return; isDragging = false;
                    const dx = e.clientX - startX;
                    if (Math.abs(dx) > 40) {
                        if (dx < 0) setLightboxImageIndex(prev => Math.min((questionsWithExplanations[reviewIndex]?.explanationImages?.length || 1) - 1, prev + 1));
                        else setLightboxImageIndex(prev => Math.max(0, prev - 1));
                    }
                };
                const onMouseLeave = () => { isDragging = false; };
                el.addEventListener('touchstart', onTouchStart, {passive:true});
                el.addEventListener('touchend', onTouchEnd, {passive:true});
                el.addEventListener('mousedown', onMouseDown);
                el.addEventListener('mouseup', onMouseUp);
                el.addEventListener('mouseleave', onMouseLeave);
                return () => {
                    el.removeEventListener('touchstart', onTouchStart);
                    el.removeEventListener('touchend', onTouchEnd);
                    el.removeEventListener('mousedown', onMouseDown);
                    el.removeEventListener('mouseup', onMouseUp);
                    el.removeEventListener('mouseleave', onMouseLeave);
                };
            }, [lightboxOpen, reviewIndex]);

            useEffect(() => {
                if (!lightboxOpen) return;
                const hk = (e) => {
                    if (e.key === 'Escape') setLightboxOpen(false);
                    if (e.key === 'ArrowRight') setLightboxImageIndex(prev => Math.min(questionsWithExplanations[reviewIndex]?.explanationImages?.length - 1 || 0, prev + 1));
                    if (e.key === 'ArrowLeft') setLightboxImageIndex(prev => Math.max(0, prev - 1));
                };
                window.addEventListener('keydown', hk);
                return () => window.removeEventListener('keydown', hk);
            }, [lightboxOpen, reviewIndex]);

            const handleRewriteQuiz = () => {
                setCurrentQuestion(0); setSelected(null); setLocked(false);
                setScore(0); setCompleted(false); setTimeIsUp(false);
                if (timerRef.current) timerRef.current.reset();
                setShowReview(false);
                setTotalTimeUsed(0); setCorrectQuestions([]); setQuizStarted(true);
            };

            const fmtTime = (t) => { if (!isFinite(t) || t <= 0) return '0:00'; const m = Math.floor(t / 60), s = Math.floor(t % 60); return m + ':' + String(s).padStart(2, '0'); };

            const renderMusicDropdown = () => {
                const dark = isDarkMode ? 'dark' : '';
                const ink = isDarkMode ? 'text-white' : 'text-gray-900';
                const pct = audioProgress.dur ? (audioProgress.cur / audioProgress.dur) * 100 : 0;
                const panelOpen = musicDropdownOpen || dropdownAnimating;
                const eqBars = (cls) => h('div', { className: 'flex items-end gap-0.5 h-4' }, [8, 12, 6].map((ht, k) => h('div', { key: k, className: `w-1 ${cls} rounded-full sound-bar`, style: { height: ht + 'px' } })));
                const miniBtn = (icon, label, fn) => h('button', { onClick: fn, 'aria-label': label, className: `liquid-glass-play-btn ${dark} w-10 h-10 flex items-center justify-center flex-shrink-0` },
                    h('span', { className: 'liquid-glass-content' }, h('i', { className: `fas ${icon} text-sm ${ink}` })));

                return h('div', { className: 'relative z-40', 'data-anchor': 'music' },
                    h('button', {
                        ref: musicBtnRef,
                        onClick: toggleMusicDropdown,
                        'aria-label': 'Music',
                        'aria-expanded': musicDropdownOpen,
                       
                        className: `liquid-glass-music-btn ${dark} px-4 md:px-5 rounded-2xl flex items-center justify-center gap-3 text-sm font-medium transition-all ${isDarkMode ? 'text-white/90' : 'text-gray-800'} ${musicDropdownOpen ? 'ring-1 ring-white/30' : ''}`,
                        style: { minWidth: isMobile ? '48px' : '200px', height: '48px', position: 'relative', zIndex: 2 }
                    },
                        h('span', { className: 'liquid-glass-content flex items-center gap-3' },
                            isPlaying ? eqBars('bg-green-400') : h('i', { className: 'fas fa-music text-sm' }),
                            !isMobile && h('span', { className: 'truncate' }, 'Songs my cat thinks are cool'),
                            !isMobile && h('i', { className: `fas fa-chevron-down text-xs transition-transform duration-300 ${musicDropdownOpen ? 'rotate-180' : ''}` })
                        )
                    ),
                    panelOpen && h('div', {
                        ref: dropdownRef,
                        className: `${isMobile ? ('dock-sheet ' + (isDarkMode ? 'sheet-dark' : 'sheet-light')) : 'absolute top-full right-0 mt-2'} overflow-visible transition-all duration-150 ${musicDropdownOpen ? 'opacity-100 scale-100' : 'opacity-0 scale-95 pointer-events-none'}`,
                        style: isMobile ? { transformOrigin: 'bottom center' } : { width: '420px', maxWidth: '90vw', transformOrigin: 'top right' }
                    },
                        h('div', { className: isMobile ? 'w-full' : '' },
                            h('div', { className: `liquid-glass-player-card ${dark} p-4 md:p-5 mb-3 md:mb-4` },
                                h('div', { className: 'liquid-glass-content' },
                                    h('div', { className: 'flex items-center gap-4' },
                                        h('div', { className: `liquid-glass-photo-frame ${dark} w-16 h-16 flex-shrink-0 cursor-pointer ${isPlaying ? 'spin-animation' : 'spin-animation paused'}`, onClick: () => playSong(currentPlaylist, 0) }, h('img', { src: currentPlaylist.cover, alt: currentPlaylist.name, className: 'w-full h-full object-cover' })),
                                        h('div', { className: 'flex-1 min-w-0' },
                                            h('p', { className: `text-xs ${isDarkMode ? 'text-gray-400' : 'text-gray-600'} mb-1` }, audioError ? "Couldn't load this track" : 'Now playing'),
                                            h('p', { className: `text-sm font-medium truncate ${ink}` }, currentPlaylist.songs[currentSongIndex]?.title || 'Select a song'),
                                            h('p', { className: `text-xs truncate ${isDarkMode ? 'text-gray-500' : 'text-gray-700'}` }, currentPlaylist.name)
                                        )
                                    ),
                                    /* Seek bar */
                                    h('div', { className: 'mt-4 flex items-center gap-3' },
                                        h('span', { className: `text-[11px] w-9 text-right font-grotesk ${isDarkMode ? 'text-gray-400' : 'text-gray-700'}` }, fmtTime(audioProgress.cur)),
                                        h('input', {
                                            type: 'range', className: `seek-range ${dark}`, min: 0, max: audioProgress.dur || 0, step: 0.1,
                                            value: Math.min(audioProgress.cur, audioProgress.dur || 0), disabled: !audioProgress.dur,
                                            style: { '--p': pct + '%' }, 'aria-label': 'Seek through song',
                                            onChange: (e) => seekTo(parseFloat(e.target.value))
                                        }),
                                        h('span', { className: `text-[11px] w-9 font-grotesk ${isDarkMode ? 'text-gray-400' : 'text-gray-700'}` }, fmtTime(audioProgress.dur))
                                    ),
                                    /* Transport */
                                    h('div', { className: 'mt-3 flex items-center justify-center gap-4' },
                                        miniBtn('fa-backward-step', 'Previous song', () => goRelative(-1)),
                                        h('button', { onClick: togglePlayPause, 'aria-label': isPlaying ? 'Pause' : 'Play', className: `liquid-glass-play-btn ${dark} w-12 h-12 flex items-center justify-center flex-shrink-0` },
                                            h('span', { className: 'liquid-glass-content' }, isPlaying ? h('i', { className: `fas fa-pause ${ink}` }) : h('i', { className: `fas fa-play ${ink}` }))
                                        ),
                                        miniBtn('fa-forward-step', 'Next song', () => goRelative(1))
                                    )
                                )
                            ),
                            h('div', { className: 'space-y-4 overflow-y-auto', style: { maxHeight: isMobile ? 'calc(52dvh - 260px)' : '55vh', paddingBottom: isMobile ? '8px' : '80px' } },
                                PLAYLISTS.map(playlist => h('div', { key: playlist.id, className: `liquid-glass-playlist-card ${dark} p-4 md:p-5` },
                                    h('div', { className: 'liquid-glass-content' },
                                        h('div', { className: 'flex items-center gap-3 mb-4' },
                                            h('div', { className: `liquid-glass-album-cover ${dark} w-14 h-14 cursor-pointer flex-shrink-0 ${currentPlaylist?.id === playlist.id && isPlaying ? 'spin-animation' : 'spin-animation paused'}`, onClick: () => playSong(playlist, 0) }, h('img', { src: playlist.cover, alt: playlist.name, className: 'w-full h-full object-cover' })),
                                            h('h4', { className: `text-base font-semibold ${ink}` }, playlist.name)
                                        ),
                                        h('div', { className: 'space-y-2' },
                                            playlist.songs.map((song, si) => {
                                                const isCur = currentPlaylist?.id === playlist.id && currentSongIndex === si;
                                                return h('button', {
                                                    key: si, onClick: () => playSong(playlist, si),
                                                    className: `w-full px-3.5 py-3 rounded-2xl transition-all text-left ${isCur ? (isDarkMode ? 'bg-white/20 border border-white/30' : 'bg-black/20 border border-black/20') : (isDarkMode ? 'bg-black/30 hover:bg-black/40' : 'bg-black/10 hover:bg-black/20')}`
                                                },
                                                    h('div', { className: 'flex items-center gap-3' },
                                                        isCur && isPlaying ? h('div', { className: 'flex items-end gap-0.5 w-6 h-5 justify-center' }, [8, 12, 6].map((ht, k) => h('div', { key: k, className: `w-1 ${isDarkMode ? 'bg-gray-300' : 'bg-gray-600'} rounded-full sound-bar`, style: { height: ht + 'px' } }))) : h('span', { className: `text-sm w-6 text-center font-medium font-grotesk ${isDarkMode ? 'text-gray-500' : 'text-gray-400'}` }, si + 1),
                                                        h('i', { className: `fas fa-music text-sm ${isDarkMode ? 'text-gray-500' : 'text-gray-400'} mr-1.5` }),
                                                        h('span', { className: `text-sm truncate ${isCur ? (isDarkMode ? 'text-white font-bold' : 'text-gray-900 font-bold') : isDarkMode ? 'text-gray-300' : 'text-gray-700'}` }, song.title)
                                                    )
                                                );
                                            })
                                        )
                                    )
                                ))
                            )
                        )
                    )
                );
            };

            const renderReviewMode = () => {
                const q = questionsWithExplanations[reviewIndex];
                const panelBg = opaqueMode ? 'bg-black/90' : 'bg-black/80';
                const lightBg = opaqueMode ? 'bg-black/60' : 'bg-black/30';
                const panelMax = isMobile ? undefined : 'calc(100dvh - 300px - var(--reserve-y, 0px))';
                const total = questionsWithExplanations.length;
                const heading = (color, badge, title) => h('div', { className: 'flex items-center gap-2 mb-4' }, badge, h('h2', { className: 'text-xl font-semibold', style: { color, fontFamily: "'Sora', sans-serif" } }, title));
                const qBadge = h('div', { className: 'w-8 h-8 rounded-full bg-[#FF9E7C] flex items-center justify-center text-black font-bold text-sm font-grotesk' }, q.index + 1);
                const eBadge = h('div', { className: 'w-8 h-8 rounded-full bg-[#7CE0FF] flex items-center justify-center' }, h('i', { className: 'fas fa-info text-black' }));
                const answerRows = (small) => h('div', { className: 'space-y-2' }, q.options.map((opt, idx) => h('div', { key: idx, className: `w-full p-3 rounded-xl border text-left ${small ? 'text-sm' : ''} ${idx === q.answer ? 'border-green-500 bg-green-500/20 text-green-300' : 'border-white/20 text-white/70'}` }, h('span', { className: 'flex items-center gap-2' }, idx === q.answer && h('i', { className: 'fas fa-check text-green-400' }), opt))));
                const protectedProps = { onContextMenu: e => e.preventDefault(), onCopy: e => e.preventDefault() };
                const count = q.explanationImages.length;

                const lightbox = lightboxOpen && count > 0 && h('div', {
                    className: 'reserve-pad absolute inset-0 z-50',
                    style: { paddingTop: '4.5rem', backgroundColor: 'rgba(0,0,0,0.86)' },
                    onClick: (e) => { if (e.target === e.currentTarget) setLightboxOpen(false); }
                },
                    h('div', { className: 'w-full h-full flex gap-3 md:gap-5 items-stretch justify-center px-3 md:px-6 pb-4 mx-auto', style: { maxWidth: '1500px', pointerEvents: 'none' } },
                        h('div', { className: `hidden lg:flex flex-col overflow-hidden flex-shrink-0`, style: { width: '28%', pointerEvents: 'auto' } },
                            h('div', { className: `${lightBg} border border-white/10 p-6 rounded-3xl overflow-y-auto h-full` },
                                heading('#FF9E7C', qBadge, 'Question'),
                                q.image && h('div', { className: 'mb-4' }, h('img', { src: q.image, alt: 'Question image', className: 'w-full object-contain rounded-2xl' })),
                                h('p', Object.assign({ className: 'mb-6 text-white whitespace-pre-wrap no-select text-sm leading-relaxed' }, protectedProps), q.question),
                                answerRows(true)
                            )
                        ),
                        h('div', {
                            ref: carouselRef,
                            className: 'flex flex-col items-center justify-center relative border border-white/10 rounded-3xl h-full flex-1 min-w-0',
                            style: { overflow: 'hidden', cursor: 'grab', backgroundColor: opaqueMode ? 'rgba(0,0,0,0.6)' : 'rgba(0,0,0,0.3)', pointerEvents: 'auto', touchAction: 'pan-y' }
                        },
                            h('div', { className: 'absolute top-3 left-1/2 -translate-x-1/2 z-20 px-3 py-1 rounded-full bg-white/10 border border-white/15' }, h('span', { className: 'text-white/70 text-xs font-medium font-grotesk' }, `${lightboxImageIndex + 1} / ${count}`)),
                            h('button', { onClick: () => setLightboxOpen(false), 'aria-label': 'Close', className: 'absolute top-3 right-3 z-20 w-10 h-10 rounded-full bg-white/10 hover:bg-white/20 border border-white/20 flex items-center justify-center' }, h('i', { className: 'fas fa-times text-white text-sm' })),
                            h('div', { className: 'relative w-full h-full flex items-center justify-center' },
                                q.explanationImages.map((imgUrl, imgIdx) => {
                                    const off = imgIdx - lightboxImageIndex;
                                    if (off < -1 || off > 1) return null;
                                    return h(motion.div, {
                                        key: imgIdx,
                                        className: 'absolute flex items-center justify-center',
                                        animate: { x: off === 0 ? '0%' : off === -1 ? '-62%' : '62%', scale: off === 0 ? 1 : 0.82, opacity: off === 0 ? 1 : 0.25 },
                                        transition: { type: 'spring', stiffness: 220, damping: 24 },
                                        style: { width: '94%', height: '94%', pointerEvents: off === 0 ? 'auto' : 'none', zIndex: off === 0 ? 10 : 5 }
                                    },
                                        h('img', { src: imgUrl, alt: `Image ${imgIdx + 1}`, draggable: false, className: 'w-full h-full object-contain rounded-2xl shadow-2xl border border-white/10' })
                                    );
                                })
                            ),
                            h('div', { className: 'absolute bottom-3 flex gap-2 items-center z-20' },
                                q.explanationImages.map((_, di) => h('button', {
                                    key: di, onClick: () => setLightboxImageIndex(di), 'aria-label': `Image ${di + 1}`,
                                    className: `rounded-full ${di === lightboxImageIndex ? 'w-6 h-2 bg-white' : 'w-2 h-2 bg-white/30 hover:bg-white/50'}`,
                                    style: { transition: 'all 0.3s var(--ease-apple)' }
                                }))
                            )
                        ),
                        h('div', { className: 'hidden lg:flex flex-col overflow-hidden flex-shrink-0', style: { width: '28%', pointerEvents: 'auto' } },
                            h('div', { className: `${lightBg} border border-white/10 p-6 rounded-3xl overflow-y-auto h-full` },
                                heading('#7CE0FF', eBadge, 'Explanation'),
                                h('p', Object.assign({ className: 'text-white whitespace-pre-wrap leading-relaxed no-select text-sm' }, protectedProps), q.explanationText)
                            )
                        )
                    )
                );

                const pagerBtn = (label, icon, disabled, fn, after) => h('button', {
                    onClick: fn, disabled,
                    className: `lg-glass-btn ${isDarkMode ? 'dark' : ''}`,
                    style: { padding: '10px 18px', display: 'flex', alignItems: 'center', gap: '8px', fontWeight: 500, fontSize: '14px', opacity: disabled ? 0.38 : 1, background: 'rgba(0,0,0,0.5)', WebkitBackdropFilter: 'blur(14px)', backdropFilter: 'blur(14px)' }
                }, !after && h('i', { className: `fas ${icon}` }), label, after && h('i', { className: `fas ${icon}` }));

                return h(React.Fragment, null,
                    h('div', { className: 'w-full h-full overflow-y-auto px-3 md:px-6 pb-6' },
                        h('div', { className: 'min-h-full flex flex-col justify-center mx-auto gap-4 md:gap-6', style: { maxWidth: '1180px' } },
                            h('div', { className: 'flex items-center justify-between gap-3 px-1' },
                                pagerBtn('Back to Results', 'fa-arrow-left', false, handleBackToResults),
                                h('span', { className: 'text-base md:text-lg font-semibold whitespace-nowrap text-white px-4 py-2 rounded-2xl', style: { background: 'rgba(0,0,0,0.5)', WebkitBackdropFilter: 'blur(14px)', backdropFilter: 'blur(14px)', fontFamily: "'Sora', sans-serif" } }, `Question ${q.index + 1} of ${questions.length}`)
                            ),
                            h('div', { className: 'flex flex-col lg:flex-row gap-3 items-stretch justify-center' },
                                h('div', { className: 'flex-1 min-w-0 max-w-xl lg:max-w-none' },
                                    h('div', { className: `${panelBg} border border-white/10 p-5 md:p-6 rounded-3xl h-full overflow-y-auto`, style: { maxHeight: panelMax } },
                                        heading('#FF9E7C', qBadge, 'Question'),
                                        q.image && h('div', { className: 'mb-4' }, h('img', { src: q.image, alt: 'Question illustration', className: 'w-full object-contain rounded-2xl' })),
                                        h('p', Object.assign({ className: 'mb-6 text-white whitespace-pre-wrap no-select' }, protectedProps), q.question),
                                        answerRows(false)
                                    )
                                ),
                                h('div', { className: 'flex-1 min-w-0 max-w-xl lg:max-w-none' },
                                    h('div', { className: `${panelBg} border border-white/10 p-5 md:p-6 rounded-3xl h-full overflow-y-auto`, style: { maxHeight: panelMax } },
                                        heading('#7CE0FF', eBadge, 'Explanation'),
                                        count > 0 && h('div', { className: 'mb-4' },
                                            h('div', { className: 'flex overflow-x-auto pb-3 gap-3 snap-x snap-mandatory', style: { scrollbarWidth: 'thin' } },
                                                q.explanationImages.map((imgUrl, imgIdx) => h('div', {
                                                    key: imgIdx, className: 'flex-shrink-0 snap-center cursor-pointer',
                                                    style: { width: '85%', maxWidth: '500px' },
                                                    onClick: () => { setLightboxOpen(true); setLightboxImageIndex(imgIdx); }
                                                }, h('img', { src: imgUrl, alt: `Explanation image ${imgIdx + 1}`, className: 'w-full h-auto object-contain rounded-2xl' })))
                                            )
                                        ),
                                        h('p', Object.assign({ className: 'text-white whitespace-pre-wrap leading-relaxed no-select' }, protectedProps), q.explanationText)
                                    )
                                )
                            ),
                            h('div', { className: 'flex items-center justify-center gap-3 md:gap-4' },
                                pagerBtn('Previous', 'fa-chevron-left', reviewIndex === 0, handlePreviousReview),
                                h('div', { className: 'flex items-center gap-2 px-2' }, questionsWithExplanations.map((_, idx) => h('div', { key: idx, className: `rounded-full ${idx === reviewIndex ? 'bg-[#FF9E7C] w-6 h-2' : isDarkMode ? 'bg-white/30 w-2 h-2' : 'bg-gray-400 w-2 h-2'}`, style: { transition: 'all 0.32s var(--ease-apple)' } }))),
                                pagerBtn('Next', 'fa-chevron-right', reviewIndex === total - 1, handleNextReview, true)
                            )
                        )
                    ),
                    lightbox
                );
            };

            const renderResultsScreen = () => {
                const la = quizHistory.length > 0 ? quizHistory[quizHistory.length - 1] : null;
                const pa = quizHistory.length > 1 ? quizHistory[quizHistory.length - 2] : null;
                const scorePercent = la ? Math.round((la.score / la.total) * 100) : Math.round((score / questions.length) * 100);
                const bestScore = quizHistory.length > 0 ? Math.max(...quizHistory.map(a => Math.round((a.score / a.total) * 100))) : scorePercent;
                const scoreDelta = pa ? Math.round(((la.score / la.total) - (pa.score / pa.total)) * 100) : null;
                const timeDelta = pa ? pa.timeUsed - la.timeUsed : null;
                const lastTime = la ? la.timeUsed : totalTimeUsed;
                const timePercent = Math.min(100, Math.round((lastTime / 1800) * 100));
                const chartScores = quizHistory.length > 0 ? quizHistory.map(a => Math.round((a.score / a.total) * 100)) : [scorePercent];
                const correctCount = correctQuestions.length;
                const avgPercent = quizHistory.length > 0 ? Math.round(quizHistory.reduce((a, b) => a + (b.score / b.total) * 100, 0) / quizHistory.length) : scorePercent;
                const totalTime = quizHistory.reduce((a, b) => a + b.timeUsed, 0) || totalTimeUsed;
                const shiftX = musicDropdownOpen ? -160 : (calcDropdownOpen ? -280 : 0);

                return h(motion.div, {
                    className: 'nexus z-10 w-full px-4 md:px-6 py-6 md:py-8 flex flex-col',
                    style: { maxWidth: 1200, maxHeight: '100%', overflowY: 'auto' }
                },
                    /* ---------- Header ---------- */
                    h(motion.div, {
                        className: 'w-full flex flex-wrap gap-4 items-end justify-between mb-7',
                        initial: { opacity: 0, y: 22 },
                        animate: { opacity: 1, y: 0 },
                        transition: { duration: 0.7, ease: EASE.outExpo }
                    },
                        h('div', null,
                            h('div', { className: 'qcard-label mb-2', style: { color: '#FF9E7C' } }, 'Session Report'),
                            h('h2', { className: 'nexus-heading text-3xl md:text-4xl text-white' }, timeIsUp ? "Time's Up" : 'Quiz Complete'),
                            h('p', { className: 'nexus-subtitle text-sm mt-2' },
                                `${questions.length} questions · Attempt #${quizHistory.length || 1} · ${new Date().toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}`)
                        ),
                        h('div', { className: 'flex gap-2.5' },
                            questionsWithExplanations.length > 0 && h('button', { onClick: handleStartReview, className: 'nexus-btn' },
                                h('i', { className: 'fas fa-file-alt text-xs' }), 'Review'),
                            h('button', { onClick: handleRewriteQuiz, className: 'nexus-btn nexus-btn-primary btn-redo' },
                                h('i', { className: 'fas fa-redo text-xs' }), 'Retry')
                        )
                    ),

                    /* ---------- Metric cards ---------- */
                    h('div', { className: 'w-full grid grid-cols-2 lg:grid-cols-4 gap-4 mb-4' },

                        /* Score — featured card with animated ring */
                        h(ResultCard, { delay: 0.08, className: 'qcard-featured p-5', style: { minHeight: 168 } },
                            h('div', { className: 'flex items-center gap-5 h-full' },
                                h(ScoreRing, { percent: scorePercent, delay: 0.5 }),
                                h('div', { className: 'flex-1 min-w-0 flex flex-col justify-center gap-1.5' },
                                    h('span', { className: 'qcard-label' }, 'Overall Score'),
                                    h('span', { className: 'text-xs nexus-muted leading-snug' }, `${la ? la.score : correctCount} of ${questions.length} correct`),
                                    scoreDelta !== null && h(PopPill, { text: `${scoreDelta >= 0 ? '▲' : '▼'} ${Math.abs(scoreDelta)}% vs prev`, tone: scoreDelta >= 0 ? 'up' : 'down', delay: 1.15 })
                                )
                            )
                        ),

                        /* Time used — animated budget bar */
                        h(ResultCard, { delay: 0.16, className: 'p-5', style: { minHeight: 168 } },
                            h('div', { className: 'flex flex-col justify-between h-full gap-3' },
                                h('div', { className: 'flex items-center justify-between' },
                                    h('span', { className: 'qcard-label' }, 'Time Used'),
                                    h('div', { className: 'qchip' }, h('i', { className: 'fas fa-stopwatch' }))
                                ),
                                h('div', null,
                                    h('span', { className: 'font-grotesk text-white font-bold leading-none', style: { fontSize: 34, letterSpacing: '-0.02em' } },
                                        h(AnimatedTime, { seconds: lastTime, duration: 1100 })),
                                    h('div', { className: 'mt-3 h-1 rounded-full', style: { background: 'rgba(255,255,255,0.08)', overflow: 'hidden' } },
                                        h(motion.div, {
                                            className: 'h-full rounded-full',
                                            style: { background: 'linear-gradient(90deg,#FF9E7C,#FFD3B8)' },
                                            initial: { width: '0%' },
                                            animate: { width: `${timePercent}%` },
                                            transition: { duration: 1.2, delay: 0.6, ease: EASE.outExpo }
                                        })
                                    ),
                                    h('div', { className: 'flex items-center justify-between mt-2 gap-2' },
                                        h('span', { className: 'text-[10px] nexus-muted' }, `${timePercent}% of 30:00 budget`),
                                        timeDelta !== null && h(PopPill, { text: `${Math.abs(timeDelta)}s ${timeDelta >= 0 ? 'faster' : 'slower'}`, tone: timeDelta >= 0 ? 'up' : 'down', delay: 1.2 })
                                    )
                                )
                            )
                        ),

                        /* Best score — shine sweep */
                        h(ResultCard, { delay: 0.24, className: 'qcard-shine p-5', style: { minHeight: 168 } },
                            h('div', { className: 'flex flex-col justify-between h-full gap-3' },
                                h('div', { className: 'flex items-center justify-between' },
                                    h('span', { className: 'qcard-label' }, 'Best Score'),
                                    h('div', { className: 'qchip' }, h('i', { className: 'fas fa-trophy' }))
                                ),
                                h('div', null,
                                    h('div', { className: 'flex items-end gap-2 flex-wrap' },
                                        h('span', { className: 'font-grotesk text-white font-bold leading-none', style: { fontSize: 34, letterSpacing: '-0.02em' } },
                                            h(AnimatedNumber, { value: bestScore, suffix: '%', duration: 1100 })),
                                        bestScore >= scorePercent && quizHistory.length > 1 && h(PopPill, { text: '★ Personal best', tone: 'up', delay: 1.25 })
                                    ),
                                    h('span', { className: 'text-xs nexus-muted mt-2 block' }, `across ${quizHistory.length || 1} attempt${quizHistory.length === 1 ? '' : 's'}`)
                                )
                            )
                        ),

                        /* Attempts — with sparkline bars */
                        h(ResultCard, { delay: 0.32, className: 'p-5', style: { minHeight: 168 } },
                            h('div', { className: 'flex flex-col justify-between h-full gap-3' },
                                h('div', { className: 'flex items-center justify-between' },
                                    h('span', { className: 'qcard-label' }, 'Attempts'),
                                    h('div', { className: 'qchip' }, h('i', { className: 'fas fa-layer-group' }))
                                ),
                                h('div', { className: 'flex items-end justify-between gap-3' },
                                    h('span', { className: 'font-grotesk text-white font-bold leading-none', style: { fontSize: 34 } },
                                        h(AnimatedNumber, { value: quizHistory.length || 1 })),
                                    h(AttemptBars, { history: quizHistory.length ? quizHistory : [{ score, total: questions.length, timestamp: 0 }], delay: 0.75 })
                                )
                            )
                        )
                    ),

                    /* ---------- Chart + breakdown ---------- */
                    h('div', { className: 'w-full grid grid-cols-1 lg:grid-cols-3 gap-4' },
                        h(HistoryPanel, { scores: chartScores, delay: 0.4 }),

                        h(ResultCard, { delay: 0.48, className: 'p-6 flex flex-col', style: { minHeight: 400 } },
                            h('div', { className: 'flex items-center justify-between mb-5' },
                                h('div', null,
                                    h('div', { className: 'qcard-label mb-1' }, 'Breakdown'),
                                    h('span', { className: 'text-sm font-semibold text-white' }, 'Question Results')
                                ),
                                h('span', { className: 'font-grotesk text-xs px-2.5 py-1 rounded-md', style: { color: '#FFB899', background: 'rgba(255,158,124,0.1)', border: '1px solid rgba(255,158,124,0.2)' } },
                                    `${correctCount}/${questions.length}`)
                            ),
                            h('div', { className: 'flex-1 flex flex-col' },
                                questions.map((_, qi) => {
                                    const isRight = correctQuestions.includes(qi);
                                    return h(motion.div, {
                                        key: qi,
                                        className: 'nexus-row',
                                        initial: { opacity: 0, x: -16 },
                                        animate: { opacity: 1, x: 0 },
                                        transition: { duration: 0.5, delay: 0.6 + qi * 0.09, ease: EASE.outExpo }
                                    },
                                        h('div', { className: 'flex items-center gap-3' },
                                            h('div', { className: 'nexus-icon-chip' },
                                                h('i', { className: `fas ${isRight ? 'fa-check' : 'fa-times'}`, style: { color: isRight ? '#4ade80' : '#f87171' } })),
                                            h('div', null,
                                                h('p', { className: 'text-sm font-semibold text-white leading-tight' }, `Question ${qi + 1}`),
                                                h('p', { className: 'text-xs nexus-muted mt-0.5' }, isRight ? 'Correct' : 'Incorrect'))
                                        ),
                                        h('span', { className: `nexus-pill ${isRight ? 'nexus-pill-up' : 'nexus-pill-down'}` }, isRight ? '✓' : '✕')
                                    );
                                })
                            ),
                            questionsWithExplanations.length > 0 && h('button', { onClick: handleStartReview, className: 'w-full nexus-btn justify-center mt-4' },
                                h('i', { className: 'fas fa-arrow-right text-xs' }), 'View Full Review')
                        )
                    ),

                    /* ---------- Bottom stats ---------- */
                    h('div', { className: 'w-full grid grid-cols-1 md:grid-cols-3 gap-4 mt-4' },
                        [
                            { label: 'Correct Answers', value: `${correctCount}/${questions.length}`, icon: 'fa-check', tint: '#4ade80', delay: 0.56 },
                            { label: 'Average Score', value: `${avgPercent}%`, icon: 'fa-chart-line', tint: '#FFB899', delay: 0.62 },
                            { label: 'Total Study Time', value: formatMS(totalTime), icon: 'fa-clock', tint: '#7CE0FF', delay: 0.68 }
                        ].map((s) => h(ResultCard, { key: s.label, delay: s.delay, className: 'px-5 py-4' },
                            h('div', { className: 'flex items-center justify-between' },
                                h('div', null,
                                    h('div', { className: 'qcard-label mb-1.5' }, s.label),
                                    h('div', { className: 'font-grotesk text-xl font-bold text-white' }, s.value)),
                                h('div', { className: 'qchip', style: { color: s.tint } }, h('i', { className: `fas ${s.icon}` }))
                            )
                        ))
                    )
                );
            };

            const renderQuestionCard = (q, i) => h(WaveQuestionCard, {
                key: i, q, i,
                isActive: i === currentQuestion,
                opaqueMode, currentQuestion, handleOption,
                musicDropdownOpen, calcDropdownOpen, isMobile,
                selected, locked, quizStarted
            });

            const toggleBtn = (onClick, label, icon, extra) => h('button', { onClick, 'aria-label': label, className: `lg-toggle ${isDarkMode ? 'dark' : ''} w-12 h-12 rounded-2xl flex items-center justify-center shadow-xl` }, icon);
            const layerIcon = h('i', { className: `fas fa-layer-group text-lg ${opaqueMode ? (isDarkMode ? 'text-gray-300' : 'text-gray-500') : (isDarkMode ? 'text-white' : 'text-gray-900')}` });
            const themeIcon = isDarkMode ? h('i', { className: 'fas fa-sun text-yellow-300 text-lg' }) : h('i', { className: 'fas fa-moon text-gray-700 text-lg' });
            const rootVars = {
                '--edge': isMobile ? '12px' : '6vw',
                '--reserve-x': reserve.x + 'px',
                '--reserve-y': reserve.y + 'px',
                '--card-max': 'calc(100dvh - 200px - var(--reserve-y, 0px))'
            };

            return h('div', { ref: rootRef, className: `quiz-root relative w-full h-full overflow-hidden flex items-center justify-center screenshot-protected ${isDarkMode ? 'bg-[#0D0D0D] text-white' : 'bg-[#F5F0EB] text-gray-900'}`, style: rootVars },
                h(AnimatePresence, null,
                    !quizStarted && h(motion.div, {
                        key: 'intro-overlay',
                        className: 'absolute inset-0 z-[70] flex items-center justify-center',
                        style: { background: 'rgba(0,0,0,0.92)', willChange: 'opacity, transform' },
                        initial: { opacity: 1 },
                        animate: { opacity: 1 },
                        exit: { opacity: 0, scale: 1.04, transition: { duration: 0.55, ease: EASE.apple } },
                        onClick: () => { clearTimeout(timeoutRef.current); setQuizStarted(true); }
                    },
                        h('div', { className: 'text-center px-6' },
                            h('span', { className: 'text-4xl md:text-6xl font-light text-white tracking-tight', style: { fontFamily: "'Sora', sans-serif" } }, typingText),
                            h('span', { className: 'typing-cursor' }),
                            h('p', { className: 'mt-8 text-xs text-white/40', style: { fontFamily: "'Manrope', sans-serif" } }, 'Tap or press Space to skip')
                        )
                    )
                ),
                musicDropdownOpen && h('div', { className: 'absolute inset-0 z-[55]', onClick: toggleMusicDropdown }),

                !showReview && h(Timer, { ref: timerRef, isDarkMode, running: quizStarted && !completed && active, duration: 1800, onExpire: handleTimeExpire }),

                h('div', { className: 'absolute z-[60] flex items-center gap-2', style: { top: '1rem', right: 'var(--edge)' } },
                    toggleBtn(toggleOpaqueMode, opaqueMode ? 'Low opacity mode' : 'High opacity mode', layerIcon),
                    toggleBtn(toggleDarkMode, 'Toggle dark mode', themeIcon),
                    h(Calculator, { isDarkMode, isMobile, isOpen: calcDropdownOpen, isAnimating: calcDropdownAnimating, onToggle: handleCalcToggle }),
                    renderMusicDropdown()
                ),

                h(FloatingBackground),

                !completed && h('div', {
                    className: 'reserve-pad absolute inset-0 z-10 flex items-center justify-center',
                    style: { paddingTop: '4.5rem' }
                }, questions.map((q, i) => renderQuestionCard(q, i))),

                quizStarted && completed && h('div', {
                    className: 'reserve-pad absolute inset-0 z-10 flex items-center justify-center',
                    style: { paddingTop: '4.5rem' }
                }, showReview ? renderReviewMode() : renderResultsScreen())
            );
        }

        window.QuizApp = PhysiologyQuiz;
        window.__quizData = { questions, explanations, gallery: [firstRowImages, secondRowImages, thirdRowImages, fourthRowImages, fifthRowImages, sixthRowImages, seventhRowImages, eighthRowImages], meta: {"3": {"title": "Rheumatoid hands", "item": 2}, "4": {"title": "Brainstem lesion", "item": 3}, "5": {"title": "Gastric pH curves", "item": 15}, "6": {"title": "Cardiac function graph", "item": 24}, "7": {"title": "Abdominal CT", "item": 26}} };
