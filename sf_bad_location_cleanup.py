"""TEMPORARY one-off cleanup script (not part of the pipeline) — archives
the Notion pages for the 173 SuccessFactors jobs confirmed live to have
location="" + a garbled title (the whole-card-in-one-<a> parsing bug fixed
in scrape_successfactors, see that function's 2026-09 BUG FIX comment).

This ID list is frozen from a query run right before this script was
written (crawl_i/crawl_ii, ats ILIKE successfactors, location blank,
is_active=true) — confirmed all have application_status='not_applied', so
nothing the user has already acted on is being touched. Notion pages are
archived (Notion's own recoverable soft-delete, not a permanent destroy)
using the exact same primitive crawl_iii.py's own stale-job cleanup uses
(notion_sync.archive_notion_pages_for_supabase_ids) so this doesn't
reinvent that logic. The Supabase-side hard-delete of these same rows is
done separately, directly via the Supabase MCP tool, AFTER this script
confirms the Notion side succeeded — same required ordering
archive_notion_pages_for_supabase_ids' own docstring documents (archive
Notion pages before the Supabase row is gone, since the row is what ties
a Notion page back to its id).

Removed after use.
"""
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")

import notion_sync

IDS = [
    68162, 70094, 70095, 70096, 70097, 70098, 70099, 70106, 70107, 70108,
    70117, 70126, 70133, 70147, 70148, 70149, 70150, 70151, 70152, 70153,
    70154, 70155, 70156, 70157, 70166, 70167, 70168, 70316, 70319, 70323,
    70325, 70326, 70327, 70330, 70331, 70332, 70337, 70338, 70340, 70342,
    70478, 70481, 70484, 70488, 70490, 70491, 70492, 70495, 70496, 70499,
    70507, 70508, 70509, 70516, 70517, 70523, 70524, 70531, 70533, 70534,
    70705, 70706, 70707, 70714, 70734, 70745, 70749, 70750, 70751, 70758,
    70920, 70921, 70924, 70925, 70926, 70929, 70930, 70937, 70938, 70940,
    70941, 70946, 71093, 71101, 71106, 71107, 71108, 71111, 71115, 71116,
    71117, 71118, 71119, 71120, 71121, 71123, 71130, 71132, 71283, 71285,
    71286, 71292, 71308, 71309, 71317, 71319, 71321, 71322, 71327, 71328,
    71329, 71330, 71332, 71468, 71469, 71473, 71474, 71475, 71478, 71479,
    71481, 71482, 71483, 71487, 71488, 71496, 71498, 71499, 71502, 71504,
    71505, 71506, 71618, 71622, 71624, 71625, 71628, 71631, 71636, 71648,
    71649, 71650, 71652, 71653, 71841, 71846, 71856, 71857, 71858, 71860,
    71868, 71869, 71870, 71871, 71872, 71874, 71875, 71876, 71882, 71892,
    71893, 71896, 71900, 71901, 71902, 71905, 71906, 71907, 71908, 71909,
    71910, 71911, 130788,
]

print(f"IDs to archive in Notion: {len(IDS)}")
archived = notion_sync.archive_notion_pages_for_supabase_ids(IDS)
print(f"DONE: archived {archived}/{len(IDS)} Notion pages")
