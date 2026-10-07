-- Delete the mixed-case duplicate boards from archive_i.
--
-- A "duplicate" here is a row like  ashby / Tempo  when  ashby / tempo  also
-- exists. The lowercase row is kept, so no board is lost. Only the ATSs in
-- slug_case.py's CASE_INSENSITIVE_SLUG_ATS are touched (keep the two lists in
-- sync). Optional: the crawler already skips these rows (supabase_handler.
-- get_all_slugs), this just removes the clutter.
--
-- Run in the Supabase SQL editor. Step 1 changes nothing; step 2 deletes.

-- ── Step 1: preview (read-only). Expect ~9,974 rows on 2026-10-07 ─────────
select a.ats, count(*) as rows_to_delete
from archive_i a
where a.ats in ('ashby','workday','smartrecruiters','greenhouse','dayforce','oracle_cloud_hcm','paycom')
  and a.slug <> lower(a.slug)
  and exists (select 1 from archive_i b where b.ats = a.ats and b.slug = lower(a.slug))
group by a.ats
order by rows_to_delete desc;

-- ── Step 2: delete. All-or-nothing; aborts if it would delete >15,000 rows ─
do $$
declare
  n bigint;
begin
  delete from archive_i a
  where a.ats in ('ashby','workday','smartrecruiters','greenhouse','dayforce','oracle_cloud_hcm','paycom')
    and a.slug <> lower(a.slug)
    and exists (select 1 from archive_i b where b.ats = a.ats and b.slug = lower(a.slug));
  get diagnostics n = row_count;
  if n > 15000 then
    raise exception 'refusing: would delete % rows (expected ~10,000); nothing was changed', n;
  end if;
  raise notice 'deleted % mixed-case duplicate boards', n;
end $$;

-- ── Step 3: verify (read-only). Expect 0 ──────────────────────────────────
select count(*) as remaining_duplicates
from archive_i a
where a.ats in ('ashby','workday','smartrecruiters','greenhouse','dayforce','oracle_cloud_hcm','paycom')
  and a.slug <> lower(a.slug)
  and exists (select 1 from archive_i b where b.ats = a.ats and b.slug = lower(a.slug));
