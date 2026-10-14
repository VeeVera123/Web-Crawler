-- Adds the exam step to profiles, richer match cards, and makes the globe and matching two-way:
-- you only see other students if you have switched your own side on.
alter table public.profiles add column if not exists step text check (step is null or step in ('Step 1', 'Step 2 CK', 'Step 2 CS', 'Step 3'));
grant update (step) on public.profiles to authenticated;

create or replace function public.community_map(min_count int default 3)
returns table(country_id text, country_name text, region text, lat smallint, lng smallint, n bigint)
language sql stable security definer set search_path = public as $$
  select p.country_id, max(p.country_name), p.region, p.lat, p.lng, count(*)
  from public.presence p
  where exists (select 1 from public.presence me where me.user_id = (select auth.uid()))
  group by p.country_id, p.region, p.lat, p.lng
  having count(*) >= greatest(coalesce(min_count, 3), 3)
$$;

create or replace function public.find_matches_v2(max_rows int default 12)
returns table(user_id uuid, display_name text, username text, avatar_url text, bio text, study_year text, step text, country_name text,
              score int, weekly_minutes int, accuracy int, streak int, exam_date date, goal_accuracy int)
language sql stable security definer set search_path = public as $$
  with me as (
    select s.* from public.public_stats s join public.profiles p on p.id = s.user_id
    where s.user_id = (select auth.uid()) and p.discoverable
  )
  select p.id, p.display_name, p.username, p.avatar_url, p.bio, p.study_year, p.step,
         (select pr.country_name from public.presence pr where pr.user_id = p.id),
         greatest(0, least(100, round(100 * (1
           - 0.30 * abs(s.weekly_minutes - me.weekly_minutes)::numeric / greatest(60, s.weekly_minutes, me.weekly_minutes)
           - 0.25 * abs(s.accuracy - me.accuracy)::numeric / 100
           - 0.15 * abs(s.streak - me.streak)::numeric / greatest(7, s.streak, me.streak)
           - 0.20 * (case when s.exam_date is not null and me.exam_date is not null then least(abs(s.exam_date - me.exam_date), 180) / 180.0 else 0.5 end)
           - 0.10 * (case when s.goal_accuracy is not null and me.goal_accuracy is not null then abs(s.goal_accuracy - me.goal_accuracy) / 100.0 else 0.3 end)
         )))::int) as score,
         s.weekly_minutes, s.accuracy::int, s.streak::int, s.exam_date, s.goal_accuracy::int
  from public.profiles p
  join public.public_stats s on s.user_id = p.id
  cross join me
  where p.discoverable and p.username is not null and p.id <> (select auth.uid())
    and not exists (select 1 from public.connections c
                    where (c.requester = (select auth.uid()) and c.addressee = p.id) or (c.addressee = (select auth.uid()) and c.requester = p.id))
  order by score desc
  limit least(greatest(max_rows, 1), 30)
$$;

create or replace function public.search_users_v2(q text)
returns table(user_id uuid, display_name text, username text, avatar_url text, bio text, study_year text, step text, country_name text)
language sql stable security definer set search_path = public as $$
  select p.id, p.display_name, p.username, p.avatar_url, p.bio, p.study_year, p.step,
         (select pr.country_name from public.presence pr where pr.user_id = p.id)
  from public.profiles p
  where exists (select 1 from public.profiles me where me.id = (select auth.uid()) and me.discoverable)
    and p.discoverable and p.username is not null and p.id <> (select auth.uid())
    and char_length(trim(q)) >= 2 and p.username ilike (replace(replace(lower(trim(q)), '%', ''), '_', '\_') || '%')
  order by p.username limit 8
$$;

create or replace function public.public_profile_v2(u text)
returns table(user_id uuid, display_name text, username text, avatar_url text, bio text, study_year text, step text, country_name text,
              weekly_minutes int, accuracy int, streak int, exam_date date, goal_accuracy int)
language sql stable security definer set search_path = public as $$
  select p.id, p.display_name, p.username, p.avatar_url, p.bio, p.study_year, p.step,
         (select pr.country_name from public.presence pr where pr.user_id = p.id),
         s.weekly_minutes, s.accuracy::int, s.streak::int, s.exam_date, s.goal_accuracy::int
  from public.profiles p left join public.public_stats s on s.user_id = p.id
  where exists (select 1 from public.profiles me where me.id = (select auth.uid()) and me.discoverable)
    and p.discoverable and lower(p.username) = lower(u)
$$;

revoke execute on function public.find_matches_v2(int), public.search_users_v2(text), public.public_profile_v2(text) from public, anon;
grant execute on function public.find_matches_v2(int), public.search_users_v2(text), public.public_profile_v2(text) to authenticated;
