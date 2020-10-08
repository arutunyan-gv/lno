------------------------------------------------------------------------------------------------------------------------
-- Добавление ансверов в задание и публикация заданий
------------------------------------------------------------------------------------------------------------------------
--
--
-- Функция на хроне
CREATE OR REPLACE FUNCTION tmp.lno_insert_answers()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
select tmp.lno_insert_answers_533();
select tmp.lno_insert_answers_1015();
select tmp.lno_insert_answers_493();
select tmp.lno_insert_answers_495();
$function$
;
select * from tmp.lno_insert_answers()
;
------------------------------------------------------------------------------------------------------------------------
-- Answers creation for 533
------------------------------------------------------------------------------------------------------------------------
--
--
CREATE OR REPLACE FUNCTION tmp.lno_insert_answers_533()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
;
with ss as (
    select id as ss_id,
           execution_id as exec_id,
           external_id as ext_id
    from power_points_snapshot
    where stage_id = 1956
      and visible_id = 2
), imp as (
    select ss_id
           ,exec_id
           ,tmp.insert_to_answer(tmp.insert_to_text_value(case when pp_id is null then 'не_указано' else pp_id end), 'TextValue', exec_id, 23564, null)
           ,tmp.insert_to_answer(tmp.insert_to_text_value(case when address is null then 'не_указано' else address end), 'TextValue', exec_id, 23567, null)
           ,tmp.insert_to_answer(case when gallery_id is null then 10630601 else gallery_id end, 'Gallery', exec_id, 23570, null)
           ,tmp.insert_to_answer(case when gallery_id is null then 10630601 else gallery_id end, 'Gallery', exec_id, 23573, null)
           ,tmp.update_power_points_by_exec_id(exec_id)
    from tmp.lno_533_valid imp
    join ss on imp.external_id = ss.ext_id
)
select * from imp
;
with ss as (
    select id as ss_id,
           execution_id as exec_id,
           external_id as ext_id
    from power_points_snapshot
    where stage_id = 1957
      and visible_id = 2
), imp as (
    select  ss_id
           ,exec_id
           ,tmp.insert_to_answer(tmp.insert_to_int_value( case when pp_install_year is null then 0 else cast(to_char((pp_install_year)::TIMESTAMP,'yyyy') as int) end), 'IntValue', exec_id, 23591, null)
           ,tmp.insert_to_answer(tmp.insert_to_text_value(case when pp_install_executor is null then 'не_указано' else pp_install_executor end), 'TextValue', exec_id, 23594, null)
           ,tmp.insert_to_answer(tmp.insert_to_text_value(case when pp_ownership_type is null then 'не_указано' else pp_ownership_type end), 'TextValue', exec_id, 23597, null)
           ,tmp.insert_to_answer(tmp.insert_to_text_value(case when pp_owner is null then 'не_указано' else pp_owner end), 'TextValue', exec_id, 23600, null)
           ,tmp.update_power_points_by_exec_id(exec_id)
    from tmp.lno_533_valid imp
    join ss on imp.external_id = ss.ext_id
)
select * from imp
;
$function$
;
------------------------------------------------------------------------------------------------------------------------
-- Answers creation for 1015
------------------------------------------------------------------------------------------------------------------------
--
--
CREATE OR REPLACE FUNCTION tmp.lno_insert_answers_1015()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
;
with ss as (
    select id as ss_id,
           execution_id as exec_id,
           external_id as ext_id
    from power_points_snapshot
    where stage_id = 2011
      and visible_id = 2
), imp as (
    select ss_id
           ,exec_id
           ,tmp.update_power_points_by_exec_id(exec_id)
    from tmp.lno_1015_valid imp
    join ss on imp.external_id = ss.ext_id
)
select * from imp
;
with ss as (
    select id as ss_id,
           execution_id as exec_id,
           external_id as ext_id
    from power_points_snapshot
    where stage_id = 2013
      and visible_id = 2
--      and execution_id = 39477032
), imp as (
    select  ss_id
            ,exec_id
            ,tmp.insert_to_answer(tmp.insert_to_text_value(okn), 'TextValue', exec_id, 25569, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(skn), 'TextValue', exec_id, 25572, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(obj_owner), 'TextValue', exec_id, 25575, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(ahp_owner), 'TextValue', exec_id, 25578, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(ahp_install), 'TextValue', exec_id, 25581, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(ahp_denial), 'TextValue', exec_id, 25584, null)
            ,tmp.update_power_points_by_exec_id(exec_id)
    from tmp.lno_1015_valid imp
    join ss on imp.external_id = ss.ext_id
)
select * from imp;
;
$function$
;
;
------------------------------------------------------------------------------------------------------------------------
-- -- Answers creation for 493
------------------------------------------------------------------------------------------------------------------------
--
--
CREATE OR REPLACE FUNCTION tmp.lno_insert_answers_493()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
;
with ss as (
    select id as ss_id,
           execution_id as exec_id,
           external_id as ext_id
    from power_points_snapshot
    where stage_id = 2005
      and visible_id = 2
), imp as (
    select  ss_id
            ,exec_id
            ,tmp.update_power_points_by_exec_id(exec_id)
    from tmp.lno_493_valid imp
    join ss on imp.external_id = ss.ext_id
)
select * from imp
;
with ss as (
    select id as ss_id,
           execution_id as exec_id,
           external_id as ext_id
    from power_points_snapshot
    where stage_id = 2008
      and visible_id = 2
), imp as (
    select  ss_id
            ,exec_id
            ,tmp.insert_to_answer(tmp.insert_to_int_value( case when support_install_year is null then 0 else cast(to_char((support_install_year)::TIMESTAMP,'yyyy') as int) end), 'IntValue', exec_id, 25077, null)
            ,tmp.insert_to_answer(tmp.insert_to_int_value( case when light_install_year is null then 0 else cast(to_char((light_install_year)::TIMESTAMP,'yyyy') as int) end), 'IntValue', exec_id, 25098, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(support_install_executor), 'TextValue', exec_id, 25080, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(light_install_executor), 'TextValue', exec_id, 25101, null)

            ,tmp.insert_to_answer(tmp.insert_to_float_value(light_power), 'FloatValue', exec_id, 25095, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(light_mark), 'TextValue', exec_id, 25092, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value('нет_данных'), 'TextValue', exec_id, 25110, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value('нет_данных'), 'TextValue', exec_id, 25089, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(support_owner), 'TextValue', exec_id, 25086, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(light_owner), 'TextValue', exec_id, 25107, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(support_ownership_type), 'TextValue', exec_id, 25083, null)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(light_ownership_type), 'TextValue', exec_id, 25104, null)
            ,tmp.update_power_points_by_exec_id(exec_id)
    from tmp.lno_493_valid imp
    join ss on imp.external_id = ss.ext_id
)
select * from imp
;
$function$
;
------------------------------------------------------------------------------------------------------------------------
-- -- Answers creation for 495
------------------------------------------------------------------------------------------------------------------------
--
--
;
CREATE OR REPLACE FUNCTION tmp.lno_insert_answers_495()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
with ss as (
    select id as ss_id,
           execution_id as exec_id,
           external_id as ext_id
    from power_points_snapshot
    where stage_id = 1959
      and visible_id = 2
), imp as (
    select  ss_id
            ,exec_id
            ,tmp.update_power_points_by_exec_id(exec_id)
    from tmp.lno_495_valid imp
    join ss on imp.external_id = ss.ext_id
)
select * from imp
;
with ss as (
    select id as ss_id,
           execution_id as exec_id,
           external_id as ext_id
    from power_points_snapshot
    where stage_id = 2006
      and visible_id = 2
), imp as (
    select  ss_id
            ,exec_id
            ,tmp.insert_to_answer(tmp.insert_to_text_value(c_cab_breakdown), 'TextValue', exec_id, 24917, null)     -- Аварийность шкафа управления
            ,tmp.insert_to_answer(tmp.insert_to_text_value(lno_owner), 'TextValue', exec_id, 24923, null)           -- Балансодержатель линии
            ,tmp.insert_to_answer(tmp.insert_to_text_value(lights_ownrer), 'TextValue', exec_id, 24926, null)       -- Балансодержатель светильников
            ,tmp.insert_to_answer(tmp.insert_to_int_value( case when install_year is null then 0 else cast(to_char((install_year)::TIMESTAMP,'yyyy') as int) end), 'IntValue', exec_id, 24887, null)    -- Год строительства/проведения капитального ремонта линии
            ,tmp.insert_to_answer(tmp.insert_to_text_value(next_check_date), 'TextValue', exec_id, 24890, null)     -- Дата следующей поверки прибора учета электроэнергии
            ,tmp.insert_to_answer(tmp.insert_to_text_value(asuno_mark), 'TextValue', exec_id, 24893, null)          -- Марка автоматизированной системы управления наружным освещением
            ,tmp.insert_to_answer(tmp.insert_to_text_value(asuno_notifications), 'TextValue', exec_id, 24905, null) -- Наличие дополнительного сервиса, обеспечивающего автоматическое формирование уведомлений для дежурно-диспетчерской службы (ДДС) и/или единого диспетчерского центра (ЕДЦ) о критических нарушениях в работе элементов систем АСУНО (короткое замыкание, перегрузка, неисправность элементов автоматизированной системы управления и т. д.)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(asuno_server), 'TextValue', exec_id, 24896, null)        -- Наличие подключения к единому серверу диспетчеризации АСУНО
            ,tmp.insert_to_answer(tmp.insert_to_text_value(asuno_acd), 'TextValue', exec_id, 24911, null)           -- Наличие сервиса адресного управления наружным освещением, позволяющего управлять каждым светильником отдельно с возможностью объединения их в группы
            ,tmp.insert_to_answer(tmp.insert_to_text_value(asuno_dimm), 'TextValue', exec_id, 24914, null)          -- Наличие функции диммирования (управления интенсивностью освещения)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(lno_id), 'TextValue', exec_id, 24872, null)              -- Номер линии наружного освещения
            ,tmp.insert_to_answer(tmp.insert_to_text_value(asuno_ac), 'TextValue', exec_id, 24908, null)            -- Обеспечение адресного управления работой элементов АСУНО
            ,tmp.insert_to_answer(tmp.insert_to_text_value(asuno_arm), 'TextValue', exec_id, 24899, null)           -- Обеспечение работы удалённого автоматизированного рабочего места диспетчера (АРМ)
            ,tmp.insert_to_answer(tmp.insert_to_text_value(asuno_consumption), 'TextValue', exec_id, 24902, null)   -- Обеспечение сбора и обработки информации с элементов системы АСУНО (качество и объем потребляемой электроэнергии, статус и т.д.)
            ,tmp.insert_to_answer(tmp.insert_to_float_value(sip_length), 'FloatValue', exec_id, 24881, null)          -- Протяженность СИП
            ,tmp.insert_to_answer(tmp.insert_to_float_value(cable_length), 'FloatValue', exec_id, 24878, null)        -- Протяженность кабельных линий
            ,tmp.insert_to_answer(tmp.insert_to_float_value(lno_length), 'FloatValue', exec_id, 24875, null)          -- Протяженность линии наружного освещения
            ,tmp.insert_to_answer(tmp.insert_to_float_value(wire_length), 'FloatValue', exec_id, 24875, null)         -- Протяженность провода
            ,tmp.insert_to_answer(tmp.insert_to_text_value(lno_ownership_type), 'TextValue', exec_id, 24920, null)  -- Тип собственности линии
            ,tmp.insert_to_answer(tmp.insert_to_geo_value(c_cab_coord_geo), 'GeoValue', exec_id, 24945, null)       -- Координаты шкафа управления
            ,tmp.update_power_points_by_exec_id(exec_id)
    from tmp.lno_495_valid imp
    join ss on imp.external_id = ss.ext_id
)
select * from imp
;
$function$
;
24945	Координаты шкафа управления
;
select * from question_component where component_type = 7
;
;
select * from answer where question_component_id = 5
;
select * from geo_value
;
select * from power_points_snapshot where stage_id = 1959
;
select * from rgis_import.log
;
select * from rgis_import.metadata

;

select qch.id,
       qch.name,
       q.id,
       q.title,
       qcom.id,
       qcom.header,
--        qcom.position
       qcom.component_type,
       qcom.required,
       qcom.*
from question_chain qch
         join question q on qch.id = q.question_chain_id
         join question_component qcom on q.id = qcom.question_id
where qch.stage_id = 2006
order by q.position, qcom.position, q.id
;
select * from power_points_snapshot where layer_id = 495

