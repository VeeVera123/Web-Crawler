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

