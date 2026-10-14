-- Opt-in community globe. Only a country is stored, never a place.
create table if not exists public.presence (
  user_id uuid primary key references auth.users(id) on delete cascade,
  country_id text not null check (country_id ~ '^[0-9]{1,3}$'),
  country_name text not null check (char_length(country_name) <= 80),
  updated_at timestamptz not null default now()
);
alter table public.presence enable row level security;

create policy "own presence read" on public.presence for select to authenticated using (user_id = (select auth.uid()));
create policy "own presence insert" on public.presence for insert to authenticated with check (user_id = (select auth.uid()));
create policy "own presence update" on public.presence for update to authenticated using (user_id = (select auth.uid())) with check (user_id = (select auth.uid()));
create policy "own presence delete" on public.presence for delete to authenticated using (user_id = (select auth.uid()));

-- the only way anyone sees other people: counts per country, and only where at least min_count (never below 3 by default)
create or replace function public.community_map(min_count int default 3)
returns table(country_id text, country_name text, n bigint)
language sql stable security definer set search_path = public as $$
  select p.country_id, max(p.country_name), count(*)
  from public.presence p
  group by p.country_id
  having count(*) >= greatest(coalesce(min_count, 3), 3)
$$;
revoke execute on function public.community_map(int) from public, anon;
grant execute on function public.community_map(int) to authenticated;
