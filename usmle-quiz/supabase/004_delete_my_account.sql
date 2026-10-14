-- Lets a signed-in person delete their own account. Their profile and synced data are removed with it.
-- The site removes the profile photo file first, then calls this function.
create or replace function public.delete_my_account() returns void language plpgsql security definer set search_path = public, auth as $$
begin
  if (select auth.uid()) is null then raise exception 'not signed in'; end if;
  delete from auth.users where id = (select auth.uid());
end $$;
revoke execute on function public.delete_my_account() from public, anon;
grant execute on function public.delete_my_account() to authenticated;
