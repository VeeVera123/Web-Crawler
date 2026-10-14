-- Run this ONCE in the Supabase SQL editor if you already ran 001 and 002 before this fix.
-- 1) The profile update policy referred to its own table, which caused "infinite recursion detected in policy".
-- 2) Replacing a photo needs permission to read the file's row, which was missing ("new row violates row-level security policy").

drop policy if exists "own profile update" on public.profiles;
create policy "own profile update" on public.profiles for update to authenticated
  using (id = (select auth.uid())) with check (id = (select auth.uid()));

revoke update on public.profiles from authenticated, anon;
grant update (display_name, username, bio, study_year, exam_date, avatar_url, last_seen) on public.profiles to authenticated;

drop policy if exists "avatar read own folder" on storage.objects;
create policy "avatar read own folder" on storage.objects for select to authenticated
  using (bucket_id = 'avatars' and (storage.foldername(name))[1] = (select auth.uid())::text);
