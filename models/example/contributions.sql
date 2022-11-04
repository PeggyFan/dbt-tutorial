
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with votes as (
  select date_trunc('date', ((created_at + interval '-7 hour'))::timestamp)::date as date, 
  user_id,
  count(*) as contributions,
  'votes' as type_
  from producthunt_db.votes 
  where created_at >= (((getdate() + interval '-7 hour')::date + interval '7 hour') - interval '89 day') and created_at < (getdate() + interval '-7 hour')::date + interval '31 hour'
  and user_id NOT IN (select id from producthunt_db.users where role IN (100))
  and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
  group by 1,2
)
, posts as (
select date_trunc('date', ((scheduled_at + interval '-7 hour'))::timestamp)::date as date, 
    user_id,
    'products' as subject, 
    count(*) as contributions,
    'posts' as type_
    from producthunt_db.posts 
    where trashed_at is null 
    and scheduled_at >= (((getdate() + interval '-7 hour')::date + interval '7 hour') - interval '89 day') and scheduled_at < (getdate() + interval '-7 hour')::date + interval '31 hour'
    and user_id NOT IN (select id from producthunt_db.users where role IN (100))
    and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
    and trashed_at is null
    group by 1, 2, 3
)
, discussions as (
    select date_trunc('date', ((created_at + interval '-7 hour'))::timestamp)::date as date, 
    user_id,
    'discussions' as subject, 
    count(*) as contributions,
    'discussions' as type_
    from producthunt_db.discussion_threads 
    where trashed_at is null 
    and created_at >= (((getdate() + interval '-7 hour')::date + interval '7 hour') - interval '89 day') and created_at < (getdate() + interval '-7 hour')::date + interval '31 hour'
    and user_id NOT IN (select id from producthunt_db.users where role IN (100))
    and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
    and status not in ('rejected', 'pending')
    and trashed_at is null
    group by 1, 2, 3
)
--     select date_trunc('date', ((created_at + interval '-7 hour'))::timestamp)::date as date, 
--     user_id,
--     'goals' as subject, 
--     count(*) as submissions
--     from producthunt_db.goals  
--     where created_at >= (((getdate() + interval '-7 hour')::date + interval '7 hour') - interval '89 day') and created_at < (getdate() + interval '-7 hour')::date + interval '31 hour'
--     and user_id NOT IN (select id from producthunt_db.users where role IN (100))
--     and user_id NOT IN (select id 
--                   from producthunt_db.users 
--                   where role IN (3, 10))
--     group by 1, 2, 3
--     ) a
--   group by 1,2

, comments as (
  select date_trunc('date', ((created_at + interval '-7 hour'))::timestamp)::date as date,  
  user_id,
  count(*) as contributions,
  'comments' as type_
  from producthunt_db.comments 
  where created_at >= (((getdate() + interval '-7 hour')::date + interval '7 hour') - interval '89 day') and created_at < (getdate() + interval '-7 hour')::date + interval '31 hour'
  and user_id NOT IN (select id from producthunt_db.users where role IN (100))
  and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
  and trashed_at is null
  group by 1,2
)
, reviews as (
  select date_trunc('date', ((created_at + interval '-7 hour'))::timestamp)::date as date, 
  user_id,
  count(*) as contributions,
  'reviews' as type
  from producthunt_db.reviews
  where created_at >= (((getdate() + interval '-7 hour')::date + interval '7 hour') - interval '89 day') and created_at < (getdate() + interval '-7 hour')::date + interval '31 hour'
  and user_id NOT IN (select id from producthunt_db.users where role IN (100))
  and user_id NOT IN (select id 
                  from producthunt_db.users 
                  where role IN (3, 10))
  group by 1,2
)
select *
from votes 
union all
select *
from submissions
union all
select *
from reviews

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
