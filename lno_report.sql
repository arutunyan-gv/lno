------------------------------------------------------------------------------------------------------------------------
-- 533
------------------------------------------------------------------------------------------------------------------------
--
--
;
with
   accepted as (
    -- Задание принято / отклонено
    select execution_id
         ,task_accepted
    from power_points_answers
    where task_accepted is not null
      and stage_id in (1957,1958)
),answers as (
    -- Ответы пользователей
    select ppa.execution_id
         ,json_agg(case when ppa.answer_rgis is null then 'пусто' else qc.header || ': ' || ppa.answer_rgis end || ' → ' || ppa.comment) as answers
    from power_points_answers ppa
    join question_component qc on ppa.question_component_id = qc.id
    where stage_id in (1956,1957)
      and (ppa.task_accepted is null and (ppa.answer_conformity = 'нет' or ppa.answer_conformity = '10') or (ppa.comment is not null))
    group by ppa.execution_id
),photo_pre as (
    -- фотографии
    select execution_id
         ,tmp.gallery_id_to_foto_link(photo_egis::int) as link
    from power_points_answers
    where stage_id in (1956,1957)
      and (task_accepted is null and (answer_conformity = 'нет' or answer_conformity = '10') and (comment is not null))
),photo as(
    select execution_id,json_agg(link) as photo
      from photo_pre
    group by execution_id
),ss_pre as (
    select external_id
         ,stage_id
         ,execution_id
         ,created_at
         ,row_number() over (partition by external_id,stage_id order by created_at desc ) as actual
         ,row_number() over (partition by external_id,stage_id order by created_at asc ) as rep
         ,actor_id
    from power_points_snapshot
    order by external_id,stage_id,execution_id,created_at asc
),ss as (
    select
           ss_pre.*
         ,actor.email as a_email
         ,actor.label as a_name
         ,execution.reserved_at
         ,execution.completed_at
         ,answers.answers
         ,photo.photo
         ,accepted.task_accepted
    from ss_pre
    join actor on ss_pre.actor_id = actor.id
    join execution on ss_pre.execution_id = execution.id
    left join answers on ss_pre.execution_id = answers.execution_id
    left join photo on ss_pre.execution_id = photo.execution_id
    left join accepted on ss_pre.execution_id = accepted.execution_id
    where actual = 1
)
select
    case   when ss_1.execution_id is null then '⚪️ Задание не создано'
           when ss_1.completed_at is null and ss_1.execution_id is not null then
               case
                   when ss_1.a_email = 'na@na.na'  then '🔴 1й Этап Верификации - 👤 В нераспределенных'
                                                    else '🔴 1й Этап Верификации - 👨 ‍В работе' end
           when ss_2.completed_at is null and ss_2.execution_id is not null then
               case
                   when ss_2.a_email = 'na@na.na'  then '🟠 2й Этап Верификации - 👤 В нераспределенных'
                                                    else '🟠 2й Этап Верификации - 👨 В работе' end
           when ss_3.completed_at is null and ss_3.execution_id is not null then
               case
                   when ss_3.a_email = 'na@na.na'  then '🟡 Модерация           - 👤 В нераспределенных'
                                                    else '🟡 Модерация           - 👨 В работе' end
                                                    else '🟢 Завершено' end::text as "Статус"

    ,common.external_id as "РГИС ID"
    ,common.omsu as "ОМСУ"
    ,common.sity as "Город"
    ,common.address as "Адрес"
    ,common.company_name as "Компания"
    ,common.pp_id as "Номер ПП"
    ,common.pp_install_year as "Год Кап Ремонта"
    ,common.pp_owner as "Собственник"
    ,common.coord as "Координаты"
    ,'🔴 Первый этап' as "🔴 Первый этап"
    ,ss_1.execution_id as "1: Задание"
    ,ss_1.a_email as "1: Логин"
    ,ss_1.a_name as "1: ФИО"
    ,ss_1.created_at as "1: Создано"
    ,ss_1.reserved_at as "1: Зарезервировано"
    ,ss_1.completed_at as "1: Завершено"
    ,case when ss_1.answers is null then '✅ Не_выявлены' else '‼️️ Выявлены' end as "1: Несоответсвия"
    ,ss_1.answers as "1: Комментарии"
    ,ss_1.photo as "Фото"
    ,'🟠 Второй Этап' as "🟠 Второй Этап"
    ,ss_2.execution_id as "2: Задание"
    ,ss_2.a_email as "2: Логин"
    ,ss_2.a_name as "2: ФИО"
    ,ss_2.created_at as "2: Создано"
    ,ss_2.reserved_at as "2: Зарезервировано"
    ,ss_2.completed_at as "2: Завершено"
    ,ss_2.answers as "2: Завершено"
    ,case when ss_2.answers is null then '✅ Не_выявлены' else '‼️️ Выявлены' end as "2: Несоответсвия"
    ,ss_1.answers as "2: Комментарии"
    ,case when ss_2.task_accepted = 'Да' then '✅ Приянто' when ss_2.task_accepted = 'Нет' then '❌ Отклонено' else ss_2.task_accepted end as "2: Подтверждено"
    ,'🟡 Третий Этап' as "🟡 Третий Этап="
    ,ss_3.execution_id as "3: Задание"
    ,ss_3.a_email as "3: Логин"
    ,ss_3.a_name as "3: ФИО"
    ,ss_3.created_at as "3: Создано"
    ,ss_3.reserved_at as "3: Зарезервировано"
    ,ss_3.completed_at as "3: Завершено"
    ,case when ss_3.task_accepted = 'Да' then '✅ Приянто' when ss_2.task_accepted = 'Нет' then '❌ Отклонено' else ss_2.task_accepted end as "3: Подтверждено"
from tmp.lno_533_valid common
left join ss ss_1 on common.external_id = ss_1.external_id and ss_1.stage_id = 1956
left join ss ss_2 on common.external_id = ss_2.external_id and ss_2.stage_id = 1957
left join ss ss_3 on common.external_id = ss_3.external_id and ss_3.stage_id = 1958
;
------------------------------------------------------------------------------------------------------------------------
-- 1015
------------------------------------------------------------------------------------------------------------------------
--
--
with
   accepted as (
    -- Задание принято / отклонено
    select execution_id
         ,task_accepted
    from power_points_answers
    where task_accepted is not null
      and stage_id in (2011,2013)
),answers as (
    -- Ответы пользователей
    select ppa.execution_id
         ,json_agg(case when ppa.answer_rgis is null then 'пусто' else qc.header || ': ' || ppa.answer_rgis end || ' → ' || ppa.comment) as answers
    from power_points_answers ppa
    join question_component qc on ppa.question_component_id = qc.id
    where stage_id in (2011,2013)
      and (ppa.task_accepted is null and (ppa.answer_conformity = 'нет' or ppa.answer_conformity = '10') or (ppa.comment is not null))
    group by ppa.execution_id
),photo_pre as (
    -- фотографии
    select execution_id
         ,tmp.gallery_id_to_foto_link(photo_egis::int) as link
    from power_points_answers
    where stage_id in (2011,2013)
      and (task_accepted is null and (answer_conformity = 'нет' or answer_conformity = '10') and (comment is not null))
),photo as(
    select execution_id,json_agg(link) as photo
      from photo_pre
    group by execution_id
),ss_pre as (
    select external_id
         ,stage_id
         ,execution_id
         ,created_at
         ,row_number() over (partition by external_id,stage_id order by created_at desc ) as actual
         ,row_number() over (partition by external_id,stage_id order by created_at asc ) as rep
         ,actor_id
    from power_points_snapshot
    order by external_id,stage_id,execution_id,created_at asc
),ss as (
    select
           ss_pre.*
         ,actor.email as a_email
         ,actor.label as a_name
         ,execution.reserved_at
         ,execution.completed_at
         ,answers.answers
         ,photo.photo
         ,accepted.task_accepted
    from ss_pre
    join actor on ss_pre.actor_id = actor.id
    join execution on ss_pre.execution_id = execution.id
    left join answers on ss_pre.execution_id = answers.execution_id
    left join photo on ss_pre.execution_id = photo.execution_id
    left join accepted on ss_pre.execution_id = accepted.execution_id
    where actual = 1
)
select
    case   when ss_1.execution_id is null then '⚪️ Задание не создано'
           when ss_1.completed_at is null and ss_1.execution_id is not null then
               case
                   when ss_1.a_email = 'na@na.na'  then '🔴 1й Этап Верификации - 👤 В нераспределенных'
                                                    else '🔴 1й Этап Верификации - 👨 ‍В работе' end
           when ss_2.completed_at is null and ss_2.execution_id is not null then
               case
                   when ss_2.a_email = 'na@na.na'  then '🟠 2й Этап Верификации - 👤 В нераспределенных'
                                                    else '🟠 2й Этап Верификации - 👨 В работе' end
           when ss_3.completed_at is null and ss_3.execution_id is not null then
               case
                   when ss_3.a_email = 'na@na.na'  then '🟡 Модерация           - 👤 В нераспределенных'
                                                    else '🟡 Модерация           - 👨 В работе' end
                                                    else '🟢 Завершено' end::text as "Статус"

    ,common.external_id as "РГИС ID"
    ,common.omsu as "ОМСУ"
    ,common.sity as "Город"
    ,common.address as "Адрес"
    ,common.company_name as "Компания"
    ,common.pp_id as "Номер ПП"
--     ,common.pp_install_year as "Год Кап Ремонта"
--     ,common.pp_owner as "Собственник"
    ,common.coord as "Координаты"
    ,'🔴 Первый этап' as "🔴 Первый этап"
    ,ss_1.execution_id as "1: Задание"
    ,ss_1.a_email as "1: Логин"
    ,ss_1.a_name as "1: ФИО"
    ,ss_1.created_at as "1: Создано"
    ,ss_1.reserved_at as "1: Зарезервировано"
    ,ss_1.completed_at as "1: Завершено"
    ,case when ss_1.answers is null then '✅ Не_выявлены' else '‼️️ Выявлены' end as "1: Несоответсвия"
    ,ss_1.answers as "1: Комментарии"
    ,ss_1.photo as "Фото"
    ,'🟠 Второй Этап' as "🟠 Второй Этап"
    ,ss_2.execution_id as "2: Задание"
    ,ss_2.a_email as "2: Логин"
    ,ss_2.a_name as "2: ФИО"
    ,ss_2.created_at as "2: Создано"
    ,ss_2.reserved_at as "2: Зарезервировано"
    ,ss_2.completed_at as "2: Завершено"
    ,ss_2.answers as "2: Завершено"
    ,case when ss_2.answers is null then '✅ Не_выявлены' else '‼️️ Выявлены' end as "2: Несоответсвия"
    ,ss_1.answers as "2: Комментарии"
    ,case when ss_2.task_accepted = 'Да' then '✅ Приянто' when ss_2.task_accepted = 'Нет' then '❌ Отклонено' else ss_2.task_accepted end as "2: Подтверждено"
    ,'🟡 Третий Этап' as "🟡 Третий Этап="
    ,ss_3.execution_id as "3: Задание"
    ,ss_3.a_email as "3: Логин"
    ,ss_3.a_name as "3: ФИО"
    ,ss_3.created_at as "3: Создано"
    ,ss_3.reserved_at as "3: Зарезервировано"
    ,ss_3.completed_at as "3: Завершено"
    ,case when ss_3.task_accepted = 'Да' then '✅ Приянто' when ss_2.task_accepted = 'Нет' then '❌ Отклонено' else ss_2.task_accepted end as "3: Подтверждено"
from tmp.lno_1015_valid common
left join ss ss_1 on common.external_id = ss_1.external_id and ss_1.stage_id = 2011
left join ss ss_2 on common.external_id = ss_2.external_id and ss_2.stage_id = 2013
left join ss ss_3 on common.external_id = ss_3.external_id and ss_3.stage_id = 2015
;

------------------------------------------------------------------------------------------------------------------------
-- 493
------------------------------------------------------------------------------------------------------------------------
--
--
with
   accepted as (
    -- Задание принято / отклонено
    select execution_id
         ,task_accepted
    from power_points_answers
    where task_accepted is not null
      and stage_id in (2005,2008)
),answers as (
    -- Ответы пользователей
    select ppa.execution_id
         ,json_agg(case when ppa.answer_rgis is null then 'пусто' else qc.header || ': ' || ppa.answer_rgis end || ' → ' || ppa.comment) as answers
    from power_points_answers ppa
    join question_component qc on ppa.question_component_id = qc.id
    where stage_id in (2005,2008)
      and (ppa.task_accepted is null and (ppa.answer_conformity = 'нет' or ppa.answer_conformity = '10') or (ppa.comment is not null))
    group by ppa.execution_id
),photo_pre as (
    -- фотографии
    select execution_id
         ,tmp.gallery_id_to_foto_link(photo_egis::int) as link
    from power_points_answers
    where stage_id in (2005,2008)
      and (task_accepted is null and (answer_conformity = 'нет' or answer_conformity = '10') and (comment is not null))
),photo as(
    select execution_id,json_agg(link) as photo
      from photo_pre
    group by execution_id
),ss_pre as (
    select external_id
         ,stage_id
         ,execution_id
         ,created_at
         ,row_number() over (partition by external_id,stage_id order by created_at desc ) as actual
         ,row_number() over (partition by external_id,stage_id order by created_at asc ) as rep
         ,actor_id
    from power_points_snapshot
    order by external_id,stage_id,execution_id,created_at asc
),ss as (
    select
           ss_pre.*
         ,actor.email as a_email
         ,actor.label as a_name
         ,execution.reserved_at
         ,execution.completed_at
         ,answers.answers
         ,photo.photo
         ,accepted.task_accepted
    from ss_pre
    join actor on ss_pre.actor_id = actor.id
    join execution on ss_pre.execution_id = execution.id
    left join answers on ss_pre.execution_id = answers.execution_id
    left join photo on ss_pre.execution_id = photo.execution_id
    left join accepted on ss_pre.execution_id = accepted.execution_id
    where actual = 1
)
select
    case   when ss_1.execution_id is null then '⚪️ Задание не создано'
           when ss_1.completed_at is null and ss_1.execution_id is not null then
               case
                   when ss_1.a_email = 'na@na.na'  then '🔴 1й Этап Верификации - 👤 В нераспределенных'
                                                    else '🔴 1й Этап Верификации - 👨 ‍В работе' end
           when ss_2.completed_at is null and ss_2.execution_id is not null then
               case
                   when ss_2.a_email = 'na@na.na'  then '🟠 2й Этап Верификации - 👤 В нераспределенных'
                                                    else '🟠 2й Этап Верификации - 👨 В работе' end
           when ss_3.completed_at is null and ss_3.execution_id is not null then
               case
                   when ss_3.a_email = 'na@na.na'  then '🟡 Модерация           - 👤 В нераспределенных'
                                                    else '🟡 Модерация           - 👨 В работе' end
                                                    else '🟢 Завершено' end::text as "Статус"

    ,common.external_id as "РГИС ID"
    ,common.omsu as "ОМСУ"
    ,common.sity as "Город"
--     ,common.address as "Адрес"
    ,common.company_name as "Компания"
--     ,common.pp_id as "Номер ПП"
--     ,common.pp_install_year as "Год Кап Ремонта"
--     ,common.pp_owner as "Собственник"
    ,common.coord as "Координаты"
    ,'🔴 Первый этап' as "🔴 Первый этап"
    ,ss_1.execution_id as "1: Задание"
    ,ss_1.a_email as "1: Логин"
    ,ss_1.a_name as "1: ФИО"
    ,ss_1.created_at as "1: Создано"
    ,ss_1.reserved_at as "1: Зарезервировано"
    ,ss_1.completed_at as "1: Завершено"
    ,case when ss_1.answers is null then '✅ Не_выявлены' else '‼️️ Выявлены' end as "1: Несоответсвия"
    ,ss_1.answers as "1: Комментарии"
    ,ss_1.photo as "Фото"
    ,'🟠 Второй Этап' as "🟠 Второй Этап"
    ,ss_2.execution_id as "2: Задание"
    ,ss_2.a_email as "2: Логин"
    ,ss_2.a_name as "2: ФИО"
    ,ss_2.created_at as "2: Создано"
    ,ss_2.reserved_at as "2: Зарезервировано"
    ,ss_2.completed_at as "2: Завершено"
    ,ss_2.answers as "2: Завершено"
    ,case when ss_2.answers is null then '✅ Не_выявлены' else '‼️️ Выявлены' end as "2: Несоответсвия"
    ,ss_1.answers as "2: Комментарии"
    ,case when ss_2.task_accepted = 'Да' then '✅ Приянто' when ss_2.task_accepted = 'Нет' then '❌ Отклонено' else ss_2.task_accepted end as "2: Подтверждено"
    ,'🟡 Третий Этап' as "🟡 Третий Этап="
    ,ss_3.execution_id as "3: Задание"
    ,ss_3.a_email as "3: Логин"
    ,ss_3.a_name as "3: ФИО"
    ,ss_3.created_at as "3: Создано"
    ,ss_3.reserved_at as "3: Зарезервировано"
    ,ss_3.completed_at as "3: Завершено"
    ,case when ss_3.task_accepted = 'Да' then '✅ Приянто' when ss_2.task_accepted = 'Нет' then '❌ Отклонено' else ss_2.task_accepted end as "3: Подтверждено"
from tmp.lno_493_valid common
left join ss ss_1 on common.external_id = ss_1.external_id and ss_1.stage_id = 2005
left join ss ss_2 on common.external_id = ss_2.external_id and ss_2.stage_id = 2008
left join ss ss_3 on common.external_id = ss_3.external_id and ss_3.stage_id = 2010
;

------------------------------------------------------------------------------------------------------------------------
-- 495
------------------------------------------------------------------------------------------------------------------------
--
--
with
   accepted as (
    -- Задание принято / отклонено
    select execution_id
         ,task_accepted
    from power_points_answers
    where task_accepted is not null
      and stage_id in (1959,2006)
),answers as (
    -- Ответы пользователей
    select ppa.execution_id
         ,json_agg(case when ppa.answer_rgis is null then 'пусто' else qc.header || ': ' || ppa.answer_rgis end || ' → ' || ppa.comment) as answers
    from power_points_answers ppa
    join question_component qc on ppa.question_component_id = qc.id
    where stage_id in (1959,2006)
      and (ppa.task_accepted is null and (ppa.answer_conformity = 'нет' or ppa.answer_conformity = '10') or (ppa.comment is not null))
    group by ppa.execution_id
),photo_pre as (
    -- фотографии
    select execution_id
         ,tmp.gallery_id_to_foto_link(photo_egis::int) as link
    from power_points_answers
    where stage_id in (1959,2006)
      and (task_accepted is null and (answer_conformity = 'нет' or answer_conformity = '10') and (comment is not null))
),photo as(
    select execution_id,json_agg(link) as photo
      from photo_pre
    group by execution_id
),ss_pre as (
    select external_id
         ,stage_id
         ,execution_id
         ,created_at
         ,row_number() over (partition by external_id,stage_id order by created_at desc ) as actual
         ,row_number() over (partition by external_id,stage_id order by created_at asc ) as rep
         ,actor_id
    from power_points_snapshot
    order by external_id,stage_id,execution_id,created_at asc
),ss as (
    select
           ss_pre.*
         ,actor.email as a_email
         ,actor.label as a_name
         ,execution.reserved_at
         ,execution.completed_at
         ,answers.answers
         ,photo.photo
         ,accepted.task_accepted
    from ss_pre
    join actor on ss_pre.actor_id = actor.id
    join execution on ss_pre.execution_id = execution.id
    left join answers on ss_pre.execution_id = answers.execution_id
    left join photo on ss_pre.execution_id = photo.execution_id
    left join accepted on ss_pre.execution_id = accepted.execution_id
    where actual = 1
)
select
    case   when ss_1.execution_id is null then '⚪️ Задание не создано'
           when ss_1.completed_at is null and ss_1.execution_id is not null then
               case
                   when ss_1.a_email = 'na@na.na'  then '🔴 1й Этап Верификации - 👤 В нераспределенных'
                                                    else '🔴 1й Этап Верификации - 👨 ‍В работе' end
           when ss_2.completed_at is null and ss_2.execution_id is not null then
               case
                   when ss_2.a_email = 'na@na.na'  then '🟠 2й Этап Верификации - 👤 В нераспределенных'
                                                    else '🟠 2й Этап Верификации - 👨 В работе' end
           when ss_3.completed_at is null and ss_3.execution_id is not null then
               case
                   when ss_3.a_email = 'na@na.na'  then '🟡 Модерация           - 👤 В нераспределенных'
                                                    else '🟡 Модерация           - 👨 В работе' end
                                                    else '🟢 Завершено' end::text as "Статус"

    ,common.external_id as "РГИС ID"
    ,common.omsu as "ОМСУ"
    ,common.sity as "Город"
--     ,common.address as "Адрес"
    ,common.company_name as "Компания"
--     ,common.pp_id as "Номер ПП"
--     ,common.pp_install_year as "Год Кап Ремонта"
--     ,common.pp_owner as "Собственник"
    ,common.coord as "Координаты"
    ,'🔴 Первый этап' as "🔴 Первый этап"
    ,ss_1.execution_id as "1: Задание"
    ,ss_1.a_email as "1: Логин"
    ,ss_1.a_name as "1: ФИО"
    ,ss_1.created_at as "1: Создано"
    ,ss_1.reserved_at as "1: Зарезервировано"
    ,ss_1.completed_at as "1: Завершено"
    ,case when ss_1.answers is null then '✅ Не_выявлены' else '‼️️ Выявлены' end as "1: Несоответсвия"
    ,ss_1.answers as "1: Комментарии"
    ,ss_1.photo as "Фото"
    ,'🟠 Второй Этап' as "🟠 Второй Этап"
    ,ss_2.execution_id as "2: Задание"
    ,ss_2.a_email as "2: Логин"
    ,ss_2.a_name as "2: ФИО"
    ,ss_2.created_at as "2: Создано"
    ,ss_2.reserved_at as "2: Зарезервировано"
    ,ss_2.completed_at as "2: Завершено"
    ,ss_2.answers as "2: Завершено"
    ,case when ss_2.answers is null then '✅ Не_выявлены' else '‼️️ Выявлены' end as "2: Несоответсвия"
    ,ss_1.answers as "2: Комментарии"
    ,case when ss_2.task_accepted = 'Да' then '✅ Приянто' when ss_2.task_accepted = 'Нет' then '❌ Отклонено' else ss_2.task_accepted end as "2: Подтверждено"
    ,'🟡 Третий Этап' as "🟡 Третий Этап="
    ,ss_3.execution_id as "3: Задание"
    ,ss_3.a_email as "3: Логин"
    ,ss_3.a_name as "3: ФИО"
    ,ss_3.created_at as "3: Создано"
    ,ss_3.reserved_at as "3: Зарезервировано"
    ,ss_3.completed_at as "3: Завершено"
    ,case when ss_3.task_accepted = 'Да' then '✅ Приянто' when ss_2.task_accepted = 'Нет' then '❌ Отклонено' else ss_2.task_accepted end as "3: Подтверждено"
from tmp.lno_495_valid common
left join ss ss_1 on common.external_id = ss_1.external_id and ss_1.stage_id = 1959
left join ss ss_2 on common.external_id = ss_2.external_id and ss_2.stage_id = 2006
left join ss ss_3 on common.external_id = ss_3.external_id and ss_3.stage_id = 2009
;
select * from power_points_snapshot where stage_id in (1959, 2006, 2009)