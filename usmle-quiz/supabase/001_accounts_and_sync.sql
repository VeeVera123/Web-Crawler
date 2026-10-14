-- Accounts and sync schema. Run once in any Supabase project (SQL editor) to recreate the backend.
-- After moving to a new project, change SB_URL and SB_KEY near the top of the "CLOUD" section in the site code.

create table public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  display_name text not null default '',
  is_admin boolean not null default false,
  created_at timestamptz not null default now(),
  last_seen timestamptz not null default now()
);
alter table public.profiles enable row level security;

create table public.user_state (
  user_id uuid not null references auth.users(id) on delete cascade,
  key text not null check (char_length(key) <= 40),
  value jsonb not null,
  updated_at timestamptz not null default now(),
  primary key (user_id, key)
);
alter table public.user_state enable row level security;

create or replace function public.is_admin()
returns boolean language sql stable security definer set search_path = public as $$
  select coalesce((select is_admin from public.profiles where id = (select auth.uid())), false);
$$;

create policy "own profile read" on public.profiles for select to authenticated using (id = (select auth.uid()));
create policy "own profile update" on public.profiles for update to authenticated
  using (id = (select auth.uid())) with check (id = (select auth.uid()) and is_admin = (select p.is_admin from public.profiles p where p.id = (select auth.uid())));
create policy "admin reads profiles" on public.profiles for select to authenticated using (public.is_admin());

create policy "own state read" on public.user_state for select to authenticated using (user_id = (select auth.uid()));
create policy "own state insert" on public.user_state for insert to authenticated with check (user_id = (select auth.uid()));
create policy "own state update" on public.user_state for update to authenticated using (user_id = (select auth.uid())) with check (user_id = (select auth.uid()));
create policy "own state delete" on public.user_state for delete to authenticated using (user_id = (select auth.uid()));

create or replace function public.handle_new_user()
returns trigger language plpgsql security definer set search_path = public as $$
begin
  insert into public.profiles (id, display_name)
  values (new.id, coalesce(new.raw_user_meta_data->>'name', split_part(new.email, '@', 1), ''));
  return new;
end $$;
revoke execute on function public.handle_new_user() from public, anon, authenticated;
create trigger on_auth_user_created after insert on auth.users for each row execute function public.handle_new_user();

create or replace function public.admin_stats()
returns jsonb language plpgsql stable security definer set search_path = public as $$
begin
  if not public.is_admin() then raise exception 'not allowed'; end if;
  return jsonb_build_object(
    'users', (select count(*) from public.profiles),
    'active_7d', (select count(*) from public.profiles where last_seen > now() - interval '7 days'),
    'new_7d', (select count(*) from public.profiles where created_at > now() - interval '7 days'),
    'notes', (select coalesce(sum(jsonb_array_length(value)),0) from public.user_state where key = 'notes' and jsonb_typeof(value) = 'array'),
    'cards', (select coalesce(sum(jsonb_array_length(value)),0) from public.user_state where key = 'cards' and jsonb_typeof(value) = 'array'),
    'attempts', (select coalesce(sum(jsonb_array_length(value)),0) from public.user_state where key = 'attempts' and jsonb_typeof(value) = 'array')
  );
end $$;
revoke execute on function public.admin_stats() from public, anon;
grant execute on function public.admin_stats() to authenticated;
revoke execute on function public.is_admin() from public, anon;
grant execute on function public.is_admin() to authenticated;
