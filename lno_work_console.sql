--====================================================================================================================--
-- Рабочая Консоль
-- arutunyan.gv@mosreg.ru
-- 20.08.2020, 19:52
--====================================================================================================================--
--






------------------------------------------------------------------------------------------------------------------------
-- Анализ текущего нейминга таблиц в БД
------------------------------------------------------------------------------------------------------------------------
--
--
select id, table_name, view_name
from md_entity
where table_name ilike 'lno_%'
order by id asc

;
select * from stage_1956_builder_sn
;
select * from stage_2011_builder_sn
;
select * from stage_2005_builder_sn
;
select * from stage_1959_builder_sn
;
select * from power_points_snapshot
;
select * from power_points_answers
;
select * from power_points_snapshot where external_id = 1257824580
;

select name || ' — ' || id from company
				where (name ilike '%ГБУСО%' or name ilike '%ГКУСО%' or name ilike '%СРЦН%')
;
select name || ' — ' || id from company where category_cd = 2 and api_service_id = 1 order by name asc
;
select * from tmp.bot_add_users
;
select * from tmp.user_check_pass('lyubeznovami@mosreg.ru')
;
select * from tmp.inu_user
;
;
;
;;
select * from tmp.insert_or_update_inu_user(276, 'Госадмтехнадзор', 3, 14, 'Начальник территориального отдела №5', 'Начальник тер. Отдела', 'Лесников Юрий Федорович ',  ' LesnikovYF@mosreg.ru ');
;
;
select * from tmp.bot_add_users
;
-- drop function  tmp.insert_or_update_inu_user(
--                                             inp_company_id int,
--                                             inp_kno text,
--                                             inp_subject int,
--                                             inp_stage_id int,
--                                             inp_position text,
--                                             inp_role text,
--                                             inp_name text,
--                                             inp_email text
--                                         )
;
CREATE OR REPLACE FUNCTION tmp.insert_or_update_inu_user(
                                            inp_company_id int,
                                            inp_kno text,
                                            inp_subject int,
                                            inp_stage_id int,
                                            inp_position text,
                                            inp_role text,
                                            inp_name text,
                                            inp_email text
                                        )
RETURNS TABLE
(
     res text
)
LANGUAGE plpgsql
SECURITY DEFINER
AS $function$
DECLARE
    inu_email text := null;
    com_name text := null;
    pass_var text := null;
    pass_count int := 0;
    return_text text := null;
    all_support_var text;
BEGIN
    select rtrim(ltrim(lower(inp_email), ' '), ' ') into inp_email
    ;
    select count(distinct pass_) into pass_count from tmp.user_check_pass('tsarevav@mosreg.ru')
    ;
    if (pass_count > 1) then
        RETURN QUERY
        (
            select E'* Для пользователя ' || case when inp_email is null then 'нет_данных' else inp_email end || ' найдено несколько паролей - устрание данную ошибку.\nВоспользуйтесь функцийями:\n    - tmp.user_check_pass(‘email’) — Показывает все пароли во всех таблицах;\n    - tmp.user_pass_update(‘email’, ‘pass’) — Меняет везде пароль на заданный;\n'
        );
    else
        select email into inu_email
        from tmp.inu_user
        where email ilike inp_email
        limit 1
        ;
        IF (inu_email is null ) THEN
            select name into com_name
            from company
            where id = inp_company_id
            ;
            select pass_ into pass_var
            from tmp.user_check_pass(inp_email)
            ;
            if (pass_var is null) then
                select 'kn' || random_between(0,9)::text || random_between(0,9)::text || random_between(0,9)::text || random_between(0,9)::text into pass_var
                ;
            end if;
            insert into tmp.inu_user (
                                      company_id
                                    , company
                                    , kno
                                    , subject_id
                                    , stage_id
                                    , pos
                                    , role
                                    , name
                                    , email
                                    , pass
                                    , is_new)
            select    inp_company_id
                    , com_name
                    , inp_kno
                    , inp_subject
                    , inp_stage_id
                    , inp_position
                    , inp_role
                    , inp_name
                    , lower(inp_email)
                    , pass_var
                    , true
            ;
            select all_support into all_support_var from all_support()
            ;
            RETURN QUERY
            (
                select 'Created: ' || case when inp_email is null then 'нет_данных' else inp_email end || ' — ' || case when pass_var is null then 'нет_данных' else pass_var end
            );
        ELSE
            update tmp.inu_user
            set deleted_at = now(), is_new = true
            where email = inp_email
            ;
            select all_support into all_support_var from all_support()
            ;
            select name into com_name from company
            where id = inp_company_id
            ;
            update tmp.inu_user
            set company_id = inp_company_id,
                company = com_name,
                kno = inp_kno,
                subject_id = inp_subject,
                stage_id = inp_stage_id,
                pos = inp_position,
                role = inp_role,
                name = inp_name,
                deleted_at = null,
                is_new = true
            where email ilike inp_email
            ;
            select all_support into all_support_var from all_support()
            ;
            select pass_ into pass_var
            from tmp.user_check_pass(inp_email)
            ;
            RETURN QUERY
            (
                select 'Updated: ' || case when inp_email is null then 'нет_данных' else inp_email end || ' — ' || case when pass_var is null then 'нет_данных' else pass_var end
            );
        end if;
    end if;
END
$function$
;
select * from power_points_snapshot where execution_id = 41573662
;
select * from tmp.inu_user where email ilike 'lesnikovyf@mosreg.ru'
;
select * from all_support()
;
select * from tmp.new_state where email ilike 'lesnikovyf@mosreg.ru'
;
select * from power_points_snapshot where layer_id = 2011
;
select check_, * from tmp.lno_493_imp where external_id in (1836435560, 1836435080, 1836434390, 1836433640, 1836432500, 1836431140, 1836430880, 1818464240)
;
select * from tmp.inu_user where email ilike 'simonenkoea@mosreg.ru'
;
select * fro
;
* Для пользователя tsarevav@mosreg.ru найдено несколько паролей - устрание данную ошибку.\nВоспользуйтесь функцийями:\n    - tmp.user_check_pass(‘email’) — Показывает все пароли во всех таблицах;\n    - tmp.user_pass_update(‘email’, ‘pass’) — Меняет везде пароль на заданный;\n

select tmp.user_check_pass('tsarevav@mosreg.ru')
;
tmp.user_pass_update(‘email’, ‘pass’)
;
    select count(distinct pass_) from tmp.user_check_pass('tsarevav@mosreg.ru')














