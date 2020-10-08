;
begin
;
rollback
;
commit
;
select * from company where name ilike '%ЛНО — %'
;
------------------------------------------------------------------------------------------------------------------------
-- Создание Компаний
------------------------------------------------------------------------------------------------------------------------
--
-- нэйминг: ЛНО — RGIS_LAIR — STAGE_ID — OMSU_NAME
;
select * from company where name ilike 'ЛНО — %' and not name ilike 'ЛНО — 533%'
;

;
-- 64 записи
with imp as (
    select distinct full_name, blago_omsu
    from tmp.oms_name_alias
    order by full_name asc
)
-- insert into company (
--                      name,
--                      search_name,
--                      subject_id,
--                      api_service_id,
--                      category_cd,
--                      updated_at,
--                      created_at,
--                      author_id,
--                      inn
--                      )
select 'ЛНО — ' || full_name as name,
       'ЛНО — ' || full_name    as search_name,
       191     as subject_id,
       3     as api_service_id,
       140     as category_cd,
       now()   as updated_at,
       now()   as created_at,
       62144,
       blago_omsu
from imp
;

;
------------------------------------------------------------------------------------------------------------------------
-- Проекты
------------------------------------------------------------------------------------------------------------------------
--
--
;
-- 64 записи
with com as (
    select *
    from company
    where name ilike 'ЛНО — %'
)
-- insert into project (company_id,
--                      name,
--                      search_name,
--                      issue_type_id,
--                      stage_id,
--                      created_at,
--                      plan_time,
--                      reserve_time,
--                      open_time,
--                      death_time
--                      )
select id,
       name,
       search_name,
       637,
       1956,
       now(),
       480,
       72,
       144,
       720
from com
;
;
-- 64 записи
with com as (
    select *
    from company
    where name ilike 'ЛНО — 533 — 1957 — %'
)
-- insert into project (company_id,
--                      name,
--                      search_name,
--                      issue_type_id,
--                      stage_id,
--                      created_at,
--                      plan_time,
--                      reserve_time,
--                      open_time,
--                      death_time
--                      )
select id,
       name,
       search_name,
       637,
       1957,
       now(),
       480,
       72,
       144,
       720
from com
;
-- 64 записи
with com as (
    select *
    from company
    where name ilike 'ЛНО — 533 — 1958 — %'
)
-- insert into project (company_id,
--                      name,
--                      search_name,
--                      issue_type_id,
--                      stage_id,
--                      created_at,
--                      plan_time,
--                      reserve_time,
--                      open_time,
--                      death_time
--                      )
select id,
       name,
       search_name,
       637,
       1958,
       now(),
       480,
       72,
       144,
       720
from com
------------------------------------------------------------------------------------------------------------------------
-- Фильтры
------------------------------------------------------------------------------------------------------------------------
--
--
;select * from project_filter_setting
-- 64
with p as (
    select *
    from project
    where stage_id = 1956
)
-- insert into project_filter_setting(
--                 project_id,
--                 filter_setting_id,
--                 role_id,
--                 ids_type,
--                 platform)
select
       p.id as project_id,
       1    as filter_setting_id,
       1    as role,
       1    as ids_type,
       'mb' as platform
from p
;
-- 64
with p as (
    select *
    from project
    where stage_id = 1957
)
-- insert into project_filter_setting(
--                 project_id,
--                 filter_setting_id,
--                 role_id,
--                 ids_type,
--                 platform)
select
       p.id as project_id,
       1    as filter_setting_id,
       1    as role,
       1    as ids_type,
       'wb' as platform
from p
;
;
-- 64
with p as (
    select *
    from project
    where stage_id = 1958
)
-- insert into project_filter_setting(
--                 project_id,
--                 filter_setting_id,
--                 role_id,
--                 ids_type,
--                 platform)
select
       p.id as project_id,
       1    as filter_setting_id,
       1    as role,
       1    as ids_type,
       'wb' as platform
from p
;