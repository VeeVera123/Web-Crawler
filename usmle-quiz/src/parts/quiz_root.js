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
