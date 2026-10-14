import re,sys
SP='/tmp/claude-0/-home-user-Web-Crawler/d9ad17b2-fbdb-50b0-9972-eadf985dc857/scratchpad/'
L=open(SP+'orig.html',encoding='utf-8').read().split('\n')   # L[0] == line 1
def seg(a,b): return '\n'.join(L[a-1:b])
def part(n): return open(SP+'parts/'+n,encoding='utf-8').read()

css=seg(44,452)          # original <style> body (lines 44-452)
quiz_lines=L[469:1897]   # lines 470..1897
# line-range replacements (bottom to top), original 1-based numbering relative to quiz_lines offset 470
def rep(a,b,new,expect):
    i=a-470; j=b-470
    assert quiz_lines[i].lstrip().startswith(expect), (a,quiz_lines[i][:80])
    quiz_lines[i:j+1]=new.rstrip('\n').split('\n')
rep(1860,1895,part('quiz_root.js'),"return h('div', { className:`min-h-screen")
rep(1495,1675,part('quiz_music.js')+'\n'+part('quiz_review.js'),"const renderMusicDropdown")
rep(1251,1311,part('quiz_audio.js'),"useEffect(() => {")
q='\n'.join(quiz_lines)

def sub(old,new,count=1,regex=False):
    global q
    if regex:
        n=len(re.findall(old,q,flags=re.S)); assert n==count,(old[:50],n)
        q=re.sub(old,lambda m:new,q,flags=re.S)
    else:
        assert q.count(old)==count,(old[:60],q.count(old)); q=q.replace(old,new)

sub("const { useState, useEffect, useRef, createElement: h, useMemo, useImperativeHandle } = React;",
    "const { useState, useEffect, useLayoutEffect, useRef, createElement: h, useMemo, useImperativeHandle, useSyncExternalStore } = React;")
# --- question cards
sub(r"const dropdownOpen = musicDropdownOpen \|\| calcDropdownOpen;.*?: 'min\(90vw, 560px\)';",
"""const dropdownOpen = musicDropdownOpen || calcDropdownOpen;
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
                : 'min(calc(100vw - var(--reserve-x, 0px) - 28px), 560px)';""",regex=True)
sub("maxHeight: isMobile ? '35vh' : '75vh'","maxHeight: isMobile ? '30dvh' : 'var(--card-max)'")
sub("style: { maxHeight: isMobile ? '40vh' : '75vh' }","style: { maxHeight: isMobile ? 'none' : 'var(--card-max)' }")
sub("overflow-y-auto max-h-[80vh] ${opaqueMode","overflow-y-auto max-h-[var(--card-max)] ${opaqueMode")
sub("style: { width: cardWidth, zIndex },","style: Object.assign({ width: cardWidth, zIndex }, isMobile ? { maxHeight: 'var(--card-max)', overflowY: 'auto', borderRadius: '24px' } : {}),")
sub("            /* Static frame whenever layout changes */",
"""            /* Re-wrap the canvas text when the card width changes (e.g. a dock panel opens) */
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

            /* Static frame whenever layout changes */""")
# --- calculator
sub("function Calculator({ isDarkMode, isOpen, isAnimating, onToggle }) {","function Calculator({ isDarkMode, isMobile, isOpen, isAnimating, onToggle }) {")
sub("h('div',{ref:wrapRef,style:{position:'relative',zIndex:9999}},","h('div',{ref:wrapRef,'data-anchor':'calc',style:{position:'relative',zIndex:9999}},")
sub("calc-pill glass-5-tilt ","calc-pill jelly ")
sub(",onMouseMove:(e)=>onTiltMove(e,btnRef),onMouseLeave:()=>onTiltLeave(btnRef)",",'data-hover':'1.08','aria-label':'Calculator'")
sub("liquid-glass-calc-panel glass-5-tilt","liquid-glass-calc-panel jelly")
sub(",onMouseMove:(e)=>onTiltMove(e,panelRef),onMouseLeave:()=>onTiltLeave(panelRef)",",'data-tilt':'3','data-hover':'1.0'")
sub("`absolute top-full right-0 mt-2 overflow-visible transition-all duration-150 ${isOpen","`${isMobile?'dock-sheet calc-sheet':'absolute top-full right-0 mt-2'} overflow-visible transition-all duration-150 ${isOpen")
sub("style:{width:'268px',transformOrigin:'top right',zIndex:9999}","style:isMobile?{transformOrigin:'bottom center'}:{width:'268px',transformOrigin:'top right',zIndex:9999}")
# --- timer + floating bg
sub("absolute z-30 px-6 py-3 rounded-2xl font-mono font-bold text-lg`, style: { top: '1rem', left: '6vw' }","absolute z-30 px-4 md:px-6 py-2 md:py-3 rounded-2xl font-mono font-bold text-base md:text-lg`, style: { top: '1rem', left: 'var(--edge, 6vw)', height: '48px', display: 'flex', alignItems: 'center' }")
sub("rows.map(row => {","rows.slice(0, isMobile ? 4 : 8).map(row => {")
# --- main component
sub("function PhysiologyQuiz() {","function PhysiologyQuiz({ active = true }) {\n            const activeRef = useRef(active); activeRef.current = active;")
sub(r"const \[isDarkMode, setIsDarkMode\] = useState\(\(\) => \{.*?\n            \}\);",
    "const [isDarkMode, setIsDarkMode] = useState(() => !!(window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches));",regex=True)
sub("const [quizHistory, setQuizHistory] = useState([]);","const quizHistory = useSyncExternalStore(window.AttemptStore.subscribe, window.AttemptStore.get);")
sub(r"useEffect\(\(\) => \{\n                if \(completed && !showReview\) \{.*?\n            \}, \[completed\]\);",
"""useLayoutEffect(() => {
                if (completed && !showReview) {
                    window.AttemptStore.add({ score, total: questions.length, timeUsed: totalTimeUsed, timestamp: Date.now(), correct: correctQuestions, timeUp: timeIsUp });
                }
            }, [completed]);""",regex=True)
sub("const hc = (e) => { setIsDarkMode(e.matches); if (e.matches) document.documentElement.classList.add('dark'); else document.documentElement.classList.remove('dark'); };","const hc = (e) => setIsDarkMode(e.matches);")
sub(r"const toggleDarkMode = \(\) => setIsDarkMode\(prev => \{.*?return nv;\n            \}\);","const toggleDarkMode = () => setIsDarkMode(prev => !prev);",regex=True)
sub("if (e.target && (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA')) return;","if (!activeRef.current) return;\n                    if (e.target && (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA')) return;")
# results
sub("""style: { maxWidth: 1200, maxHeight: '100vh', overflowY: 'auto' },
                    animate: { x: shiftX },
                    transition: { duration: DUR.med, ease: EASE.apple }
                },""","""style: { maxWidth: 1200, maxHeight: '100%', overflowY: 'auto' }
                },""")
sub("className: 'w-full flex items-end justify-between mb-7',","className: 'w-full flex flex-wrap gap-4 items-end justify-between mb-7',")
sub("className: 'nexus z-10 w-full px-6 py-8 flex flex-col',","className: 'nexus z-10 w-full px-4 md:px-6 py-6 md:py-8 flex flex-col',")
# expose
sub("const FEEDBACK_MS = 420;","const FEEDBACK_MS = 420;",1)
q += "\n        window.QuizApp = PhysiologyQuiz;\n        window.__quizData = { questions, explanations };\n"
open(SP+'quiz_patched.js','w',encoding='utf-8').write(q)

svg=seg(457,458)
html=part('head.html')+"    <style>\n"+css+"\n"+part('quiz.css')+"\n    </style>\n</head>\n<body class=\"quiz-locked\">\n"+svg+"\n<div id=\"root\"></div>\n<script>"+part('core.js')+"</script>\n<script>\n(() => {\n"+q+"\n})();\n</script>\n<script>ReactDOM.createRoot(document.getElementById('root')).render(React.createElement('div',{style:{position:'fixed',inset:0}},React.createElement(window.QuizApp,{active:true})));</script>\n</body>\n</html>\n"
open(SP+'quiz_only.html','w',encoding='utf-8').write(html)
print('ok',len(html))
