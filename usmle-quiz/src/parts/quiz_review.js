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
