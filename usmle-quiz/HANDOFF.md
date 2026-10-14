# USMLE study site: handoff notes

One static page (`index.html`) built from source parts. Edit the parts, never `index.html` directly.

## Build
`python3 usmle-quiz/src/parts/build.py` writes `usmle-quiz/index.html` (also `src/site.html`).
Inputs: `src/orig.html` (the original quiz), `src/parts/*` (head, css, core.js, patched quiz pieces, site.js, site2.css,
s_photos.js and s_friends.js which hold sample photos as data URIs).

## Stack
React 18 UMD + htm (no build tooling), hash router (#/home|quiz|dashboard|study|library|community|account), Supabase
(supabase-js 2.109.0, passkeys on). Project URL and the publishable key are in `site.js` (safe in the browser, RLS protects data).
SQL migrations: `usmle-quiz/supabase/001..008`. 008 is the latest (profiles.step, two-way community_map, v2 match RPCs).

## Key pieces in site.js
- Stores (localStorage), cloud sync (`SYNC`, `pullAndMerge`, `pushKey`: writes only on change, 20s debounce, throttled),
  auth modal, profile modal, account page, study tools, community globe (globe.gl), Connect layer
  (match deck, find, People home, friend page, chat), profile dropdown drip (physics in AccountButton).
- Quiz is gated behind sign-in (`quizOk`). Liquid glass = `.glass` (tint ::before, backdrop displacement ::after, no background/border).

## State / pending
- Needs real hosting for passkeys and GitHub login. Supabase: set Site URL, Redirect URLs, Passkey RP ID/origin.
- Matching, requests and chat were only tested against a fake client; test with two real accounts.
- To do: report/block users, admin panel (aggregate stats), notifications, move location lookup server-side.
- Sample matches and friends are fake (photos in s_photos.js / s_friends.js) until real users exist.
