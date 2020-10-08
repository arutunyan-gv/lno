------------------------------------------------------------------------------------------------------------------------
-- ЛНО импорт
------------------------------------------------------------------------------------------------------------------------
--
--
-- Статус реестров импорта
select external_id, status_cd, entity_id, updated_at
from rgis_import.metadata
where external_id in (493, 495, 533, 1015)
order by external_id asc
;
;
;
-- Реестры импора
select id, table_name, view_name
from md_entity
where id in (5450, 5448, 5449, 5447)
order by id asc
;
------------------------------------------------------------------------------------------------------------------------
-- Вспомогательные запросы
------------------------------------------------------------------------------------------------------------------------
--
--
-- Просмотр QC
select qch.stage_id,
       qch.id,
       qch.name,
       q.position,
       q.id,
       q.title,
       qcom.id,
       qcom.header,
       qcom.required,
       qcom.component_type,
       qcom.component_type,
       qcom.position
from question_chain qch
         join question q on qch.id = q.question_chain_id
         join question_component qcom on q.id = qcom.question_id
where qch.stage_id in (1959)
order by q.id desc, qcom.position asc
;
;
;
-- Просмотр атрибутов таблицы
select  ',' || a.name || E'\t -- ' ||o.name
        ,validation, *
from md_entity e
join md_entity_attribute a on e.id = a.entity_id
join entity_attribute_option o on a.id = o.entity_attribute_id
where e.table_name = 'md_entity'
order by a.name
;
------------------------------------------------------------------------------------------------------------------------
-- 533 — Сведения о пунктах питания наружного освещения
------------------------------------------------------------------------------------------------------------------------
--
--
-- вьюха для просмотра импорта
;
-- drop view tmp.lno_533_imp cascade
;
create or replace view tmp.lno_533_imp as (
select     '0️⃣ Служебное:' as "_Служебное:"
         ,external_id
         ,created_at
         ,updated_at
         ,deleted_at
         ,erorrs
         ,files
         ,gallery_id
--       ,geo
         ,'1️⃣ Данные:'    as "Данные:"
         ,rg_20239         as check_              -- Верификация в МП
         ,'2️⃣ Локация:'   as "Локация:"
         ,rg_8772          as coord_system        -- Система координат
         ,rg_10242         as geo_type            -- Тип геометрии
         ,rg_8256          as coord               -- Координаты
         ,tmp.varchar_coordinates_to_geography(rg_8256) as geo
         ,case when rg_9439 is null  then 'нет_данных' else rg_9439 end          as omsu                -- Муниципальное образование
         ,case when rg_18167 is null  then 'нет_данных' else rg_18167 end         as sity               -- Город
         ,case when rg_9431 is null  then 'нет_данных' else rg_9431 end          as address             -- Адрес пункта питания
         ,'3️⃣ Проверка:'  as "Проверка:"
         ,rg_9429          as pp_id               -- Номер пункта питания (трансформаторной подстанции)
         ,rg_9432          as pp_install_year     -- Год строительства/проведения капитального ремонта пункта питания (трансформаторной подстанции)
         ,rg_9433          as pp_install_executor -- Организация,проводившая установку/ капитальный ремонт пункта питания (трансформаторной подстанции)
         ,rg_9434          as pp_ownership_type   -- Тип собственности на пункт питания (трансформаторную подстанцию)
         ,rg_9435          as pp_owner            -- Собственник пункта питания (трансформаторной подстанции)
    from rgis_import.doc_533_cache
)

;
-- Вьюха для просмотра валидных записей
--
-- drop view tmp.lno_533_valid cascade
;
create or replace view tmp.lno_533_valid as
(
with c as (
    select *
    from company
    where name ilike 'ЛНО%'
      and subject_id = 191
      and category_cd = 140
      and api_service_id = 3
),
     com as (
         select c.id        as c_id
              , c.name      as c_name
              , p.id        as p_id
              , p.name      as p_name
              , c.omsu_id
              , c.id        as com_2_id
              , c.name      as com_2_name
              , c.id        as com_3_id
              , c.name      as com_3_name
         from c
                  join project p on p.stage_id = 1956 and c.id = p.company_id
--                   join c com_2 on c.omsu_id = com_2.omsu_id and com_2.name ilike '%1957%'
--                   join c com_3 on c.omsu_id = com_3.omsu_id and com_3.name ilike '%1958%'
         where c.name ilike 'ЛНО — %' and not c.name ilike '%533%'
     )
select imp.*,
       '4️⃣ Дополнительно'             as "_Дополнительно",
       alias.full_name                 as omsu_name,
       alias.blago_omsu::int           as omsu_id,
       1956                            as stage_id,
       '533-' || imp.external_id::text as name,
       '533-' || imp.external_id::text as search_name,
       com.c_id                        as company_id,
       com.c_name                      as company_name,
       com.p_id                        as project_id,
       com.p_name                      as project_name,
       com_2_id,
       com_2_name,
       com_3_id,
       com_3_name,
       62144                           as author_id,
       0                               as actuality,
       -1                              as actor_id,
       2                               as visible_id,
       -1                              as location_id,
       geo::geography                  as location,
       533                             as layer_id
from tmp.lno_533_imp imp
         join tmp.oms_name_alias alias on imp.omsu ilike '%' || alias.name || '%'
         join com on alias.blago_omsu = com.omsu_id::int
where imp.check_ = 'да'
  and geo is not null
)
;
select * from tmp.lno_533_valid
;
------------------------------------------------------------------------------------------------------------------------
-- 1015 — Сведения об объектах архитектурно-художественной подсветки (АХП)
------------------------------------------------------------------------------------------------------------------------
--
--
drop view tmp.lno_1015_imp cascade
;
create or replace view tmp.lno_1015_imp as
(
    select '0️⃣ Служебное:' as "Служебное"
         ,id
         ,external_id
         ,created_at
         ,updated_at
         ,deleted_at
         ,erorrs
         ,files
         ,gallery_id
--          ,geo
         ,'1️⃣ Данные:'    as "Данные"
         ,rg_20242     as check_           -- Верификация в МП
         ,'2️⃣ Локация:'   as "Локация"
         ,rg_8772      as coord_system     -- Система координат
         ,rg_10242     as geo_type         -- Тип геометрии
         ,rg_8256      as coord            -- Координаты
         ,tmp.varchar_coordinates_to_geography(rg_8256) as geo
         ,rg_17920     as omsu             -- Муниципальное образование
         ,rg_18166     as sity             -- Город
         ,rg_17921     as locality         -- Населенный пункт
         ,rg_17611     as address          -- Адрес объекта
         ,'3️⃣ Проверка:'  as "Проверка"
         ,rg_17922     as obj_name         -- Название объекта
         ,rg_17923     as obj_type         -- Тип объекта АХП
         ,rg_17926     as obj_owner        -- Собственник объекта АХП
         ,rg_17924     as okn              -- Объект культурного наследия (ОКН)
         ,rg_17925     as skn              -- Объект культурного наследия в которых размещаются объекты социально-досуговой инфраструктуры (СДИ)
         ,rg_17610     as pp_id            -- Номер пункта питания (если балансодержатель АХП – муниципалитет)
         ,rg_17612     as light_type       -- Тип источника света - не используется,ролям не добавлять!
         ,rg_17613     as power            -- Установленная мощность (если балансодержатель АХП – муниципалитет)
         ,rg_17615     as power_source     -- Способ питания светильников - не используется,ролям не добавлять!
         ,rg_17929     as accounting_id    -- Номер прибора учёта (если балансодержатель АХП – муниципалитет)
         ,rg_17930     as lights_count     -- Количество светильников (если балансодержатель АХП – муниципалитет)
         ,rg_17927     as ahp_id           -- Номер шкафа управления (если балансодержатель АХП – муниципалитет)
         ,rg_17928     as ahp_coord       -- Координаты шкафа управления  (если балансодержатель АХП – муниципалитет)
         ,tmp.varchar_coordinates_to_geography(rg_17928) as ahp_coord_geo
         ,rg_17931     as ahp_install      -- Возможность установки АХП
         ,rg_17932     as ahp_denial       -- Причина невозможности АХП
         ,rg_17933     as ahp_availability -- Наличие системы АХП
         ,rg_18013     as ahp_owner        -- Собственник системы АХП
    from rgis_import.doc_1015_cache
)
;
select * from tmp.lno_1015_imp
;
-- Вьюха для просмотра валидных записей
--
-- drop view tmp.lno_1015_valid cascade
;
create or replace view tmp.lno_1015_valid as
(
with c as (
    select *
    from company
    where name ilike 'ЛНО%'
      and subject_id = 191
      and category_cd = 140
      and api_service_id = 3
),
     com as (
         select c.id        as c_id
              , c.name      as c_name
              , p.id        as p_id
              , p.name      as p_name
              , c.omsu_id
              , c.id        as com_2_id
              , c.name      as com_2_name
              , c.id        as com_3_id
              , c.name      as com_3_name
         from c
                  join project p on p.stage_id = 2011 and c.id = p.company_id
         where c.name ilike 'ЛНО — %' and not c.name ilike '%533%'
     )
select
        imp.id
        ,imp.external_id
        ,imp.created_at
        ,imp.updated_at
        ,imp.deleted_at
        ,imp.erorrs
        ,imp.files
        ,case when imp.gallery_id is null then 10630601 else imp.gallery_id end
        ,imp."Данные"
        ,imp.check_           -- Верификация в МП
        ,imp."Локация"
        ,imp.coord_system     -- Система координат
        ,imp.geo_type         -- Тип геометрии
        ,imp.coord            -- Координаты
        ,imp.geo
        ,case when imp.omsu is null then 'нет_данных' else imp.omsu end            -- Муниципальное образование
        ,case when imp.sity is null then 'нет_данных' else imp.sity end             -- Город
        ,case when imp.locality is null then 'нет_данных' else imp.locality end         -- Населенный пункт
        ,case when imp.address is null then 'нет_данных' else imp.address end          -- Адрес объекта
        ,imp."Проверка"
        ,case when imp.obj_name is null then 'нет_данных' else imp.obj_name end          -- Название объекта
        ,case when imp.obj_type is null then 'нет_данных' else imp.obj_type end        -- Тип объекта АХП
        ,case when imp.obj_owner is null then 'нет_данных' else imp.obj_owner end      -- Собственник объекта АХП
        ,case when imp.okn is null then 'нет_данных' else imp.okn end            -- Объект культурного наследия (ОКН)
        ,case when imp.skn is null then 'нет_данных' else imp.skn end            -- Объект культурного наследия в которых размещаются объекты социально-досуговой инфраструктуры (СДИ)
        ,case when imp.pp_id is null then 'нет_данных' else imp.pp_id end          -- Номер пункта питания (если балансодержатель АХП – муниципалитет)
        ,case when imp.light_type is null then 'нет_данных' else imp.light_type end     -- Тип источника света - не используется, ролям не добавлять!
        ,case when imp.power is null then 0.0 else imp.power end          -- Установленная мощность (если балансодержатель АХП – муниципалитет)
        ,case when imp.power_source is null then 'нет_данных' else imp.power_source end   -- Способ питания светильников - не используется, ролям не добавлять!
        ,case when imp.accounting_id is null then 'нет_данных' else imp.accounting_id end  -- Номер прибора учёта (если балансодержатель АХП – муниципалитет)
        ,case when imp.lights_count is null then 0 else imp.lights_count end   -- Количество светильников (если балансодержатель АХП – муниципалитет)
        ,case when imp.ahp_id is null then 'нет_данных' else imp.ahp_id end          -- Номер шкафа управления (если балансодержатель АХП – муниципалитет)
        ,case when imp.ahp_coord is null then 'нет_данных' else imp.ahp_coord end      -- Координаты шкафа управления  (если балансодержатель АХП – муниципалитет)
        ,case when imp.ahp_coord_geo is null then '0101000020E6100000000000B865B142409102B8EF3FE84B40'::geometry else imp.ahp_coord_geo end
        ,case when imp.ahp_install is null then 'нет_данных' else imp.ahp_install end    -- Возможность установки АХП
        ,case when imp.ahp_denial is null then 'нет_данных' else imp.ahp_denial end     -- Причина невозможности АХП
        ,case when imp.ahp_availability is null then 'нет_данных' else imp.ahp_availability end -- Наличие системы АХП
        ,case when imp.ahp_owner is null then 'нет_данных' else imp.ahp_owner end       -- Собственник системы АХП
        ,'4️⃣ Дополнительно'             as "_Дополнительно"
        ,alias.full_name                 as omsu_name
        ,alias.blago_omsu::int           as omsu_id
        ,2011                            as stage_id
        ,'1015-' || imp.external_id::text as name
        ,'1015-' || imp.external_id::text as search_name
        ,com.c_id                        as company_id
        ,com.c_name                      as company_name
        ,com.p_id                        as project_id
        ,com.p_name                      as project_name
        ,com_2_id
        ,com_2_name
        ,com_3_id
        ,com_3_name
        ,62144                           as author_id
        ,0                               as actuality
        ,-1                              as actor_id
        ,2                               as visible_id
        ,-1                              as location_id
        ,geo::geography                  as location
        ,1015                            as layer_id
from tmp.lno_1015_imp imp
         join tmp.oms_name_alias alias on imp.omsu ilike '%' || alias.name || '%'
         join com on alias.blago_omsu = com.omsu_id::int
where imp.check_ = 'да'
  and geo is not null
)
;
------------------------------------------------------------------------------------------------------------------------
-- 493 — Сведения о светильниках и опорах наружного освещения
------------------------------------------------------------------------------------------------------------------------
--
--
;
select * from rgis_import.doc_493_cache
;
drop view tmp.lno_493_imp cascade
;
create or replace view tmp.lno_493_imp as (
    select
'0️⃣ Служебное:'    as "Служебное"
    , id
    , external_id
    , created_at
    , updated_at
    , deleted_at
    , erorrs
    , files
    , gallery_id
--     , geo
, '1️⃣ Данные:'     as "Данные"
    , rg_20241	as check_                       -- Верификация в МП
, '2️⃣ Локация:'    as "Локация"
    , rg_8772	as coord_system                 -- Система координат
    , rg_10242	as geo_type                     -- Тип геометрии
    , rg_8256	as coord                        -- Координаты
    , tmp.varchar_coordinates_to_geography(rg_8256) as geo
    , rg_9437	as omsu                         -- Муниципальное образование
    , rg_18168	as sity                         -- Город
    , rg_18661	as locality                     -- Населенный пункт
, '3️⃣ Проверка:'  as "Проверка"
    , rg_11215	as support_id                   -- Порядковый номер опоры от пункта питания (трансформаторной подстанции)
    , rg_8635	as support_light_num            -- Количество светильников на опоре (заполняется в отдельном блоке, не удалено специально)
    , rg_18636	as support_bracket_num          -- Количество кронштейнов
    , rg_9421	as support_mark                 -- Марка опоры
    , rg_9422	as support_material             -- Материал опоры
    , rg_18633	as support_height               -- Высота опоры освещения
    , rg_9423	as support_install_year         -- Год установки/проведения капитального ремонта опоры
    , rg_9424	as support_install_executor     -- Организация, проводившая установку/ капитальный ремонт опоры
    , rg_18646	as support_breakdown            -- Аварийность опоры
    , rg_9425	as support_ownership_type       -- Тип собственности на опору
    , rg_9426	as support_owner                -- Собственник опоры
    , rg_9427	as support_add_eq_1             -- Наличие дополнительного оборудования 1
    , rg_18666	as support_add_eq_2             -- Наличие дополнительного оборудования 2
    , rg_18667	as support_add_eq_3             -- Наличие дополнительного оборудования 3
    , rg_18668	as support_add_eq_4             -- Наличие дополнительного оборудования 4
    , rg_8636	as light_mark                   -- Марка светильника (заполняется в отдельном блоке, не удалено специально)
    , rg_8637	as light_type                   -- Тип лампы (заполняется в отдельном блоке, не удалено специально)
    , rg_9416	as light_power                  -- Единичная мощность светильника (заполняется в отдельном блоке, не удалено специально)
    , rg_19090	as light_direct_control         -- Наличие сервиса адресного управления наружным освещением, позволяющего управлять светильником отдельно с возможностью объединения их в группы.
    , rg_19091	as light_dimm                   -- Наличие функции диммирования (управления интенсивностью освещения).
    , rg_9417	as light_install_year           -- Год установки/проведения капитального ремонта светильника (заполняется в отдельном блоке, не удалено специально)
    , rg_9418	as light_install_executor       -- Организация, проводившая установку/ капитальный ремонт светильника (заполняется в отдельном блоке, не удалено специально)
    , rg_9428	as light_feed_type              -- Способ питания светильника (заполняется в отдельном блоке, не удалено специально)
    , rg_9419	as light_ownership_type         -- Тип собственности на светильник (заполняется в отдельном блоке, не удалено специально)
    , rg_9420	as light_owner                  -- Собственник светильника (заполняется в отдельном блоке, не удалено специально)
    , rg_18634	as cable_length                 -- Длина тросового подвеса
    , rg_18635	as cable_height                 -- Высота тросового подвеса
from rgis_import.doc_493_cache
)
;
select * from tmp.lno_493_imp
;
-- drop view tmp.lno_493_valid
;
create or replace view tmp.lno_493_valid as
(
with c as (
    select *
    from company
    where name ilike 'ЛНО%'
      and subject_id = 191
      and category_cd = 140
      and api_service_id = 3
),
     com as (
         select c.id        as c_id
              , c.name      as c_name
              , p.id        as p_id
              , p.name      as p_name
              , c.omsu_id
              , c.id        as com_2_id
              , c.name      as com_2_name
              , c.id        as com_3_id
              , c.name      as com_3_name
         from c
                  join project p on p.stage_id = 2005 and c.id = p.company_id
         where c.name ilike 'ЛНО — %' and not c.name ilike '%533%'
     )
select   imp."Служебное"
        ,imp.id
        ,imp.external_id
        ,imp.created_at
        ,imp.updated_at
        ,imp.deleted_at
        ,imp.erorrs
        ,imp.files
        ,case when imp.gallery_id is null then 10630601 else imp.gallery_id end
        ,imp."Данные"
        ,imp.check_           -- Верификация в МП
        ,imp."Локация"
        ,imp.coord_system     -- Система координат
        ,imp.geo_type         -- Тип геометрии
        ,imp.coord            -- Координаты
        ,imp.geo
        ,case when imp.omsu is null then 'нет_данных' else imp.omsu end                     -- Муниципальное образование
        ,case when imp.sity is null then 'нет_данных' else imp.sity end                     -- Город
        ,case when imp.locality is null then 'нет_данных' else imp.locality end             -- Населенный пункт
        ,case when imp.support_id is null then 0 else imp.support_id end                    -- Порядковый номер опоры от пункта питания
        ,imp."Проверка"
        ,case when imp.support_light_num is null then 0 else imp.support_light_num end                      -- Количество светильников на опоре (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.support_bracket_num is null then 0 else imp.support_bracket_num end                  -- Количество кронштейнов
        ,case when imp.support_mark is null then 'нет_данных' else imp.support_mark end                     -- Марка опоры
        ,case when imp.support_material is null then 'нет_данных' else imp.support_material end             -- Материал опоры
        ,case when imp.support_height is null then 0 else imp.support_height end                            -- Высота опоры освещения
        ,case when imp.support_install_year is null then now() else imp.support_install_year end            -- Год установки/проведения капитального ремонта опоры
        ,case when imp.support_install_executor is null then 'нет_данных' else imp.support_install_executor end   -- Номер пункта питания (если балансодержатель АХП – муниципалитет)
        ,case when imp.support_breakdown is null then 'нет_данных' else imp.support_breakdown end           -- Аварийность опоры
        ,case when imp.support_ownership_type is null then 'нет_данных' else imp.support_ownership_type end -- Тип собственности на опору
        ,case when imp.support_owner is null then 'нет_данных' else imp.support_owner end                   -- Собственник опоры
        ,case when imp.support_add_eq_1 is null then 'нет_данных' else imp.support_add_eq_1 end             -- Наличие дополнительного оборудования 1
        ,case when imp.support_add_eq_2 is null then 'нет_данных' else imp.support_add_eq_2 end             -- Наличие дополнительного оборудования 2
        ,case when imp.support_add_eq_3 is null then 'нет_данных' else imp.support_add_eq_3 end             -- Наличие дополнительного оборудования 3
        ,case when imp.support_add_eq_4 is null then 'нет_данных' else imp.support_add_eq_4 end             -- Наличие дополнительного оборудования 4
        ,case when imp.light_mark is null then 'нет_данных' else imp.light_mark end                         -- Марка светильника (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.light_power is null then 0 else imp.light_power end                                  -- Единичная мощность светильника (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.light_direct_control is null then 'нет_данных' else imp.light_direct_control end     -- Наличие сервиса адресного управления наружным освещением, позволяющего управлять светильником отдельно с возможностью объединения их в группы.
        ,case when imp.light_dimm is null then 'нет_данных' else imp.light_dimm end                         -- Наличие функции диммирования (управления интенсивностью освещения).
        ,case when imp.light_install_year is null then now() else imp.light_install_year end                -- Год установки/проведения капитального ремонта светильника (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.light_install_executor is null then 'нет_данных' else imp.light_install_executor end -- Организация, проводившая установку/ капитальный ремонт светильника (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.light_feed_type is null then 'нет_данных' else imp.light_feed_type end               -- Способ питания светильника (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.light_ownership_type is null then 'нет_данных' else imp.light_ownership_type end     -- Тип собственности на светильник (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.light_owner is null then 'нет_данных' else imp.light_owner end                       -- Собственник светильника (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.light_type is null then 'нет_данных' else imp.light_type end                         -- Тип лампы (заполняется в отдельном блоке, не удалено специально)
        ,case when imp.cable_length is null then 0 else imp.cable_length end                                -- Длина тросового подвеса
        ,case when imp.cable_height is null then 0 else imp.cable_height end                                -- Высота тросового подвеса
        ,'4️⃣ Дополнительно'             as "_Дополнительно"
        ,alias.full_name                 as omsu_name
        ,alias.blago_omsu::int           as omsu_id
        ,2005                            as stage_id
        ,'493-' || imp.external_id::text as name
        ,'493-' || imp.external_id::text as search_name
        ,com.c_id                        as company_id
        ,com.c_name                      as company_name
        ,com.p_id                        as project_id
        ,com.p_name                      as project_name
        ,com_2_id
        ,com_2_name
        ,com_3_id
        ,com_3_name
        ,62144                           as author_id
        ,0                               as actuality
        ,-1                              as actor_id
        ,2                               as visible_id
        ,-1                              as location_id
        ,geo::geography                  as location
        ,493                             as layer_id
from tmp.lno_493_imp imp
         join tmp.oms_name_alias alias on imp.omsu ilike '%' || alias.name || '%'
         join com on alias.blago_omsu = com.omsu_id::int
where imp.check_ = 'да'
  and geo is not null
)
;
------------------------------------------------------------------------------------------------------------------------
-- 495 — Сведения о линиях наружного освещения
------------------------------------------------------------------------------------------------------------------------
--
--
;
;
drop view tmp.lno_495_imp cascade
;
create or replace view tmp.lno_495_imp as
(
select '0️⃣ Служебное:'      as "Служебное"
     ,id
     ,external_id
     ,created_at
     ,updated_at
     ,deleted_at
     ,erorrs
     ,files
     ,gallery_id
--     ,geo
     ,'1️⃣ Данные:'         as "Данные"
     ,rg_20240          as check_              -- Верификация в МП
     ,'2️⃣Локация:'        as "Локация"
     ,rg_8772           as coord_system        -- Система координат
     ,rg_10242          as geo_type            -- Тип геометрии
     ,rg_8256           as coord               -- Координаты
     ,tmp.varchar_coordinates_to_geography(rg_8256) as geo
     ,rg_9438           as omsu                -- Муниципальное образование
     ,rg_18118          as sity                -- Город
     ,rg_18111          as locality            -- Населенный пункт
     ,rg_18124          as address             -- Адрес объекта
     ,'3️⃣ Проверка:'    as "_Проверка"
     ,rg_8638           as lno_id              -- Номер линии наружного освещения
     ,rg_8640           as lno_address         -- Адреса,по которым проходит линия наружного освещения
     ,rg_8639           as pp_id               -- Номер пункта питания
     ,rg_8641           as lno_length          -- Протяженность линии наружного освещения
     ,rg_8644           as cable_length        -- Протяженность кабельных линий
     ,rg_8645           as sip_length          -- Протяженность СИП
     ,rg_9411           as wire_length         -- Протяженность провода
     ,rg_9412           as install_year        -- Год строительства/проведения капитального ремонта линии
     ,rg_8642           as support_num         -- Количество опор освещения
     ,rg_8643           as lamp_num            -- Количество светильников
     ,rg_18112          as c_cab_coord         -- Координаты Шкафа управления
     ,tmp.varchar_coordinates_to_geography(rg_18112) as c_cab_coord_geo
     ,rg_18113          as c_cab_info          -- Номер / название / маркировка Шкафа управления
     ,rg_18114          as asuno               -- Наличие АСУНО в ШУ
     ,rg_9413           as accounting_mark     -- Марка прибора учета электроэнергии
     ,rg_18115          as pu_id               -- Номер ПУ
     ,rg_9414           as next_check_date     -- Дата следующей поверки прибора учета электроэнергии
     ,rg_9415           as asuno_mark          -- Марка автоматизированной системы управления наружным освещением
     ,rg_19092          as asuno_server        -- Наличие подключения к единому серверу диспетчеризации АСУНО.
     ,rg_19093          as asuno_arm           -- Обеспечение работы удалённого автоматизированного рабочего места диспетчера (АРМ). Выбор из списка: Да,нет.
     ,rg_19094          as asuno_consumption   -- Обеспечение сбора и обработки информации с элементов системы АСУНО (качество и объем потребляемой электроэнергии,статус и т.д.).
     ,rg_19095          as asuno_notifications -- Наличие дополнительного сервиса,обеспечивающего автоматическое формирование уведомлений для дежурно-диспетчерской службы (ДДС) и/или единого диспетчерского центра (ЕДЦ) о критических нарушениях в работе элементов систем АСУНО (короткое замыкание,перегрузка,неисправность элементов автоматизированной системы управления и т. д.).
     ,rg_19096          as asuno_ac            -- (adress control) Обеспечение адресного управление работой элементов АСУНО.
     ,rg_19097          as asuno_acd           -- (address control directly) Наличие сервиса адресного управления наружным освещением,позволяющего управлять каждым светильником отдельно с возможностью объединения их в группы.
     ,rg_19098          as asuno_dimm          -- Наличие функции диммирования (управления интенсивностью освещения).
     ,rg_18116          as c_cab_breakdown     -- Аварийность Шкафа управления
     ,rg_18117          as comment             -- Комментарий
     ,rg_18125          as contr               -- Реквизиты контракта,срок действия
     ,rg_18126          as contr_mun           -- Реквизиты контракта/ муниципального задания,период действия с ____по
     ,rg_18127          as contr_mun_work      -- Виды работ,прописанные в контракте/муниципальном задании
     ,rg_18128          as contr_mun_price     -- Цена контракта/муниципального задания
     ,rg_18129          as contr_exec          -- По контракту ссылка на закупку/по муниципальному заданию
     ,rg_18130          as lno_ownership_type  -- Тип собственности линии
     ,rg_18131          as lno_owner           -- Балансодержатель линии
     ,rg_18132          as lno_serv_com        -- Обслуживающая организация линии
     ,rg_18133          as lights_serv_com     -- Обслуживающая организация светильников
     ,rg_18134          as lights_ownrer       -- Балансодержатель светильников
from rgis_import.doc_495_cache
    )
;
select * from tmp.lno_495_imp
;
;
;
-- Вьюха для просмотра валидных записей
--
-- drop view tmp.lno_495_valid cascade
;
create or replace view tmp.lno_495_valid as
(
with c as (
    select *
    from company
    where name ilike 'ЛНО%'
      and subject_id = 191
      and category_cd = 140
      and api_service_id = 3
),
     com as (
         select c.id        as c_id
              , c.name      as c_name
              , p.id        as p_id
              , p.name      as p_name
              , c.omsu_id
              , c.id        as com_2_id
              , c.name      as com_2_name
              , c.id        as com_3_id
              , c.name      as com_3_name
         from c
                  join project p on p.stage_id = 1959 and c.id = p.company_id
         where c.name ilike 'ЛНО — %' and not c.name ilike '%533%'
     )
select
        "Служебное"
        ,external_id
        ,created_at
        ,updated_at
        ,deleted_at
        ,erorrs
        ,files
        ,gallery_id
        ,"Данные"
        ,check_              -- Верификация в МП
        ,"Локация"
        ,coord_system        -- Система координат
        ,geo_type            -- Тип геометрии
        ,coord               -- Координаты
        ,geo
        ,case when omsu is null then 'нет_данных' else omsu end                                 -- Муниципальное образование
        ,case when sity is null then 'нет_данных' else sity end                                 -- Город
        ,case when locality is null then 'нет_данных' else locality end                         -- Населенный пункт
        ,case when address is null then 'нет_данных' else address end                           -- Адрес объекта
        ,"_Проверка"
        ,case when lno_id is null then 'нет_данных' else lno_id end                             -- Номер линии наружного освещения
        ,case when lno_address is null then 'нет_данных' else lno_address end                   -- Адреса,по которым проходит линия наружного освещения
        ,case when pp_id is null then 'нет_данных' else pp_id end                               -- Номер пункта питания
        ,case when lno_length is null then 0 else lno_length end                                -- Протяженность линии наружного освещения
        ,case when cable_length is null then 0 else cable_length end                            -- Протяженность кабельных линий
        ,case when sip_length is null then 0 else sip_length end                                -- Протяженность СИП
        ,case when wire_length is null then 0 else wire_length end                              -- Протяженность провода
        ,case when install_year is null then null else install_year end                 -- Год строительства/проведения капитального ремонта линии
        ,case when support_num is null then 0 else support_num end                              -- Количество опор освещения
        ,case when lamp_num is null then 0 else lamp_num end                                    -- Количество светильников
        ,c_cab_coord                                                                            -- Координаты Шкафа управления
        ,c_cab_coord_geo                                                                        -- Координаты Шкафа управления
        ,case when c_cab_info is null then 'нет_данных' else c_cab_info end                     -- Номер / название / маркировка Шкафа управления
        ,case when asuno is null then 'нет_данных' else asuno end                               -- Наличие АСУНО в ШУ
        ,case when accounting_mark is null then 'нет_данных' else accounting_mark end           -- Марка прибора учета электроэнергии
        ,case when pu_id is null then 'нет_данных' else pu_id end                               -- Номер ПУ
        ,case when next_check_date is null then 'нет_данных' else next_check_date::date::text end                   -- Дата следующей поверки прибора учета электроэнергии
        ,case when asuno_mark is null then 'нет_данных' else asuno_mark end                     -- Марка автоматизированной системы управления наружным освещением
        ,case when asuno_server is null then 'нет_данных' else asuno_server end                 -- Наличие подключения к единому серверу диспетчеризации АСУНО.
        ,case when asuno_arm is null then 'нет_данных' else asuno_arm end                       -- Обеспечение работы удалённого автоматизированного рабочего места диспетчера (АРМ). Выбор из списка: Да,нет.
        ,case when asuno_consumption is null then 'нет_данных' else asuno_consumption end       -- Обеспечение сбора и обработки информации с элементов системы АСУНО (качество и объем потребляемой электроэнергии,статус и т.д.).
        ,case when asuno_notifications is null then 'нет_данных' else asuno_notifications end   -- Наличие дополнительного сервиса,обеспечивающего автоматическое формирование уведомлений для дежурно-диспетчерской службы (ДДС) и/или единого диспетчерского центра (ЕДЦ) о критических нарушениях в работе элементов систем АСУНО (короткое замыкание,перегрузка,неисправность элементов автоматизированной системы управления и т. д.).
        ,case when asuno_ac is null then 'нет_данных' else asuno_ac end                         -- (adress control) Обеспечение адресного управление работой элементов АСУНО.
        ,case when asuno_acd is null then 'нет_данных' else asuno_acd end                       -- (address control directly) Наличие сервиса адресного управления наружным освещением,позволяющего управлять каждым светильником отдельно с возможностью объединения их в группы.
        ,case when asuno_dimm is null then 'нет_данных' else asuno_dimm end                     -- Наличие функции диммирования (управления интенсивностью освещения).
        ,case when c_cab_breakdown is null then 'нет_данных' else c_cab_breakdown end           -- Аварийность Шкафа управления
        ,case when comment is null then 'нет_данных' else comment end                           -- Комментарий
        ,case when contr is null then 'нет_данных' else contr end                               -- Реквизиты контракта,срок действия
        ,case when contr_mun is null then 'нет_данных' else contr_mun end                       -- Реквизиты контракта/ муниципального задания,период действия с ____по
        ,case when contr_mun_work is null then 'нет_данных' else contr_mun_work end             -- Виды работ,прописанные в контракте/муниципальном задании
        ,case when contr_mun_price is null then 'нет_данных' else contr_mun_price end           -- Цена контракта/муниципального задания
        ,case when contr_exec is null then 'нет_данных' else contr_exec end                     -- По контракту ссылка на закупку/по муниципальному заданию
        ,case when lno_ownership_type is null then 'нет_данных' else lno_ownership_type end     -- Тип собственности линии
        ,case when lno_owner is null then 'нет_данных' else lno_owner end                       -- Балансодержатель линии
        ,case when lno_serv_com is null then 'нет_данных' else lno_serv_com end                 -- Обслуживающая организация линии
        ,case when lights_serv_com is null then 'нет_данных' else lights_serv_com end           -- Обслуживающая организация светильников
        ,case when lights_ownrer is null then 'нет_данных' else lights_ownrer end               -- Балансодержатель светильников
       ,'4️⃣ Дополнительно'             as "_Дополнительно",
       alias.full_name                 as omsu_name,
       alias.blago_omsu::int           as omsu_id,
       2011                            as stage_id,
       '495-' || imp.external_id::text as name,
       '495-' || imp.external_id::text as search_name,
       com.c_id                        as company_id,
       com.c_name                      as company_name,
       com.p_id                        as project_id,
       com.p_name                      as project_name,
       com_2_id,
       com_2_name,
       com_3_id,
       com_3_name,
       62144                           as author_id,
       0                               as actuality,
       -1                              as actor_id,
       2                               as visible_id,
       -1                              as location_id,
       geo                             as location,
       495                            as layer_id
from tmp.lno_495_imp imp
         join tmp.oms_name_alias alias on imp.omsu ilike '%' || alias.name || '%'
         join com on alias.blago_omsu = com.omsu_id::int
where imp.check_ = 'да'
  and geo is not null
)
;
select * from  tmp.lno_495_imp where external_id  = 1257824580
;
select * from data_processor where name ilike 'ЛНО — %'
;
select * from stage_1959_builder_sn;