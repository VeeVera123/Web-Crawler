-- Adds a state or province and a rounded area (whole degrees, about 100 km) to community presence.
-- Run once in the Supabase SQL editor. Together with 004_delete_my_account.sql this is everything still pending.
alter table public.presence
  add column if not exists region text check (region is null or char_length(region) <= 80),
  add column if not exists lat smallint check (lat is null or lat between -90 and 90),
  add column if not exists lng smallint check (lng is null or lng between -180 and 180);

drop function if exists public.community_map(int);
create function public.community_map(min_count int default 3)
returns table(country_id text, country_name text, region text, lat smallint, lng smallint, n bigint)
language sql stable security definer set search_path = public as $$
  select p.country_id, max(p.country_name), p.region, p.lat, p.lng, count(*)
  from public.presence p
  group by p.country_id, p.region, p.lat, p.lng
  having count(*) >= greatest(coalesce(min_count, 3), 3)
$$;
revoke execute on function public.community_map(int) from public, anon;
grant execute on function public.community_map(int) to authenticated;
