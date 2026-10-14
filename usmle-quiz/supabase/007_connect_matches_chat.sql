-- Matching, connections and chat. Everything is opt-in: nobody is matched or searchable until they switch it on.

alter table public.profiles add column if not exists discoverable boolean not null default false;
grant update (discoverable) on public.profiles to authenticated;

-- a small summary other students can be matched on (never the raw history)
create table public.public_stats (
  user_id uuid primary key references auth.users(id) on delete cascade,
  weekly_minutes int not null default 0 check (weekly_minutes between 0 and 10080),
  accuracy smallint not null default 0 check (accuracy between 0 and 100),
  streak smallint not null default 0 check (streak between 0 and 3650),
  exam_date date,
  goal_accuracy smallint check (goal_accuracy between 0 and 100),
  weekly_goal_hours smallint check (weekly_goal_hours between 0 and 168),
  updated_at timestamptz not null default now()
);
alter table public.public_stats enable row level security;
create policy "own stats read" on public.public_stats for select to authenticated using (user_id = (select auth.uid()));
create policy "own stats insert" on public.public_stats for insert to authenticated with check (user_id = (select auth.uid()));
create policy "own stats update" on public.public_stats for update to authenticated using (user_id = (select auth.uid())) with check (user_id = (select auth.uid()));
create policy "own stats delete" on public.public_stats for delete to authenticated using (user_id = (select auth.uid()));

create table public.connections (
  id uuid primary key default gen_random_uuid(),
  requester uuid not null references auth.users(id) on delete cascade,
  addressee uuid not null references auth.users(id) on delete cascade,
  status text not null default 'pending' check (status in ('pending', 'accepted')),
  created_at timestamptz not null default now(),
  check (requester <> addressee)
);
create unique index connections_pair on public.connections (least(requester, addressee), greatest(requester, addressee));
alter table public.connections enable row level security;
create policy "see own connections" on public.connections for select to authenticated using ((select auth.uid()) in (requester, addressee));
create policy "accept a request" on public.connections for update to authenticated using (addressee = (select auth.uid())) with check (addressee = (select auth.uid()) and status = 'accepted');
create policy "remove a connection" on public.connections for delete to authenticated using ((select auth.uid()) in (requester, addressee));
-- requests are created only through request_connection(), which checks the other person is discoverable

create table public.messages (
  id bigint generated always as identity primary key,
  conn_id uuid not null references public.connections(id) on delete cascade,
  sender uuid not null references auth.users(id) on delete cascade,
  body text not null check (char_length(body) between 1 and 2000),
  created_at timestamptz not null default now()
);
create index messages_conn on public.messages (conn_id, created_at);
alter table public.messages enable row level security;
create policy "read chat" on public.messages for select to authenticated
  using (exists (select 1 from public.connections c where c.id = conn_id and c.status = 'accepted' and (select auth.uid()) in (c.requester, c.addressee)));
create policy "send chat" on public.messages for insert to authenticated
  with check (sender = (select auth.uid()) and exists (select 1 from public.connections c where c.id = conn_id and c.status = 'accepted' and (select auth.uid()) in (c.requester, c.addressee)));
alter publication supabase_realtime add table public.messages;

-- best matches for me: closeness in study time, scores, streak, exam date and goals (0 to 100)
create or replace function public.find_matches(max_rows int default 12)
returns table(user_id uuid, display_name text, username text, avatar_url text, bio text, study_year text, country_name text,
              score int, weekly_minutes int, accuracy int, streak int, exam_date date, goal_accuracy int)
language sql stable security definer set search_path = public as $$
  with me as (
    select s.* from public.public_stats s join public.profiles p on p.id = s.user_id
    where s.user_id = (select auth.uid()) and p.discoverable
  )
  select p.id, p.display_name, p.username, p.avatar_url, p.bio, p.study_year,
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

create or replace function public.search_users(q text)
returns table(user_id uuid, display_name text, username text, avatar_url text, bio text, study_year text)
language sql stable security definer set search_path = public as $$
  select p.id, p.display_name, p.username, p.avatar_url, p.bio, p.study_year
  from public.profiles p
  where (select auth.uid()) is not null and p.discoverable and p.username is not null and p.id <> (select auth.uid())
    and char_length(trim(q)) >= 2 and p.username ilike (replace(replace(lower(trim(q)), '%', ''), '_', '\_') || '%')
  order by p.username limit 8
$$;

create or replace function public.public_profile(u text)
returns table(user_id uuid, display_name text, username text, avatar_url text, bio text, study_year text, country_name text,
              weekly_minutes int, accuracy int, streak int, exam_date date, goal_accuracy int)
language sql stable security definer set search_path = public as $$
  select p.id, p.display_name, p.username, p.avatar_url, p.bio, p.study_year,
         (select pr.country_name from public.presence pr where pr.user_id = p.id),
         s.weekly_minutes, s.accuracy::int, s.streak::int, s.exam_date, s.goal_accuracy::int
  from public.profiles p left join public.public_stats s on s.user_id = p.id
  where (select auth.uid()) is not null and p.discoverable and lower(p.username) = lower(u)
$$;

create or replace function public.my_connections()
returns table(conn_id uuid, status text, i_requested boolean, other_id uuid, display_name text, username text, avatar_url text, last_body text, last_at timestamptz)
language sql stable security definer set search_path = public as $$
  select c.id, c.status, c.requester = (select auth.uid()),
         o.id, o.display_name, o.username, o.avatar_url,
         (select m.body from public.messages m where m.conn_id = c.id order by m.created_at desc limit 1),
         (select m.created_at from public.messages m where m.conn_id = c.id order by m.created_at desc limit 1)
  from public.connections c
  join public.profiles o on o.id = case when c.requester = (select auth.uid()) then c.addressee else c.requester end
  where (select auth.uid()) in (c.requester, c.addressee)
  order by coalesce((select max(m.created_at) from public.messages m where m.conn_id = c.id), c.created_at) desc
$$;

create or replace function public.request_connection(target uuid)
returns void language plpgsql security definer set search_path = public as $$
begin
  if (select auth.uid()) is null then raise exception 'not signed in'; end if;
  if not exists (select 1 from public.profiles where id = target and discoverable) then raise exception 'that student is not available'; end if;
  insert into public.connections (requester, addressee) values ((select auth.uid()), target) on conflict do nothing;
end $$;

revoke execute on function public.find_matches(int), public.search_users(text), public.public_profile(text), public.my_connections(), public.request_connection(uuid) from public, anon;
grant execute on function public.find_matches(int), public.search_users(text), public.public_profile(text), public.my_connections(), public.request_connection(uuid) to authenticated;
