-- Profile fields (username, bio, study year, exam date, photo) and the avatars storage bucket.
-- Additive only: to undo, drop the new columns/policies and delete the 'avatars' bucket.
alter table public.profiles
  add column username text,
  add column bio text not null default '',
  add column study_year text,
  add column exam_date date,
  add column avatar_url text;

alter table public.profiles
  add constraint username_format check (username is null or username ~ '^[a-z0-9_]{3,20}$'),
  add constraint username_reserved check (username is null or username not in ('admin','administrator','support','root','moderator','official','staff')),
  add constraint bio_len check (char_length(bio) <= 160),
  add constraint study_year_ok check (study_year is null or study_year in ('MS1','MS2','MS3','MS4','Gap year','Graduate','Other'));

create unique index profiles_username_key on public.profiles (lower(username));

create or replace function public.username_available(u text)
returns boolean language sql stable security definer set search_path = public as $$
  select not exists (
    select 1 from public.profiles
    where lower(username) = lower(u)
      and id <> coalesce((select auth.uid()), '00000000-0000-0000-0000-000000000000'::uuid)
  );
$$;
revoke execute on function public.username_available(text) from public;
grant execute on function public.username_available(text) to anon, authenticated;

insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values ('avatars', 'avatars', true, 262144, array['image/jpeg','image/png','image/webp'])
on conflict (id) do nothing;

grant update (username, bio, study_year, exam_date, avatar_url) on public.profiles to authenticated;

create policy "avatar read own folder" on storage.objects for select to authenticated
  using (bucket_id = 'avatars' and (storage.foldername(name))[1] = (select auth.uid())::text);
create policy "avatar insert own folder" on storage.objects for insert to authenticated
  with check (bucket_id = 'avatars' and (storage.foldername(name))[1] = (select auth.uid())::text);
create policy "avatar update own folder" on storage.objects for update to authenticated
  using (bucket_id = 'avatars' and (storage.foldername(name))[1] = (select auth.uid())::text)
  with check (bucket_id = 'avatars' and (storage.foldername(name))[1] = (select auth.uid())::text);
create policy "avatar delete own folder" on storage.objects for delete to authenticated
  using (bucket_id = 'avatars' and (storage.foldername(name))[1] = (select auth.uid())::text);
