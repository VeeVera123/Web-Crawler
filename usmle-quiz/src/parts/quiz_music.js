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
