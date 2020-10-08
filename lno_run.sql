------------------------------------------------------------------------------------------------------------------------
-- Запросы для подготовки ЛНО
------------------------------------------------------------------------------------------------------------------------
--
--
-- Запускать все по очереди
-- Запускать инсерты после проверки
;
begin
;
rollback
;
commit
;
;
;
grant all privileges on
    stage_1956_builder_sn,
    stage_1956_builder_sn_id_seq,
    stage_1959_builder_sn,
    stage_1959_builder_sn_id_seq,
    stage_2005_builder_sn,
    stage_2005_builder_sn_id_seq,
    stage_2011_builder_sn,
    stage_2011_builder_sn_id_seq
to ro_user
;
;
;
-- 128 шт
delete from company
where name ilike 'ЛНО — 533%'
;
;
;
-- 64 шт
update company
set search_name = name
where name ilike 'ЛНО — %' and not name ilike 'ЛНО — 533%'
;
;
;
-- 64 омсу * 9 этапов = 576 проектов
with stages (lair, stage) as (
    select 1015, 1959 union all
    select 1015, 2006 union all
    select 1015, 2009 union all
    select 493,  2005 union all
    select 493,  2008 union all
    select 493,  2010 union all
    select 495,  2011 union all
    select 495,  2013 union all
    select 495,  2015
),com as (
    select id, name, split_part(name, ' — ', 2) as city
    from company
    where name ilike 'ЛНО — %' and not name ilike 'ЛНО — 533%'
)
-- insert into project (
--                      company_id,
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
select com.id,
       'ЛНО — ' || stages.lair || ' — ' || stages.stage || ' — ' || com.city,
       'ЛНО — ' || stages.lair || ' — ' || stages.stage || ' — ' || com.city,
       637,
       stages.stage,
       now(),
       480,
       72,
       144,
       720
from com, stages
;
;
;
-- 192
with p as (
    select *
    from project
    where stage_id in (1959, 2005, 2011) and name ilike 'ЛНО — %'
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
;
;
-- 384
with p as (
    select *
    from project
    where stage_id in (2006, 2009, 2008, 2010, 2013, 2015) and name ilike 'ЛНО — %'
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
;




