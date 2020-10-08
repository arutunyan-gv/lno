------------------------------------------------------------------------------------------------------------------------
-- Наполенеие стэйджбилдера
------------------------------------------------------------------------------------------------------------------------
--
--
-- Функция на хроне
CREATE OR REPLACE FUNCTION tmp.lno_import()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
select tmp.lno_533_ins_to_sb();
select tmp.lno_1015_ins_to_sb();
select tmp.lno_493_ins_to_sb();
select tmp.lno_495_ins_to_sb();
$function$
;
select * from tmp.lno_import()
;

------------------------------------------------------------------------------------------------------------------------
-- tmp.lno_533_ins_to_sb()
------------------------------------------------------------------------------------------------------------------------
--
--
;
-- drop function tmp.lno_533_ins_to_sb()
;
CREATE OR REPLACE FUNCTION tmp.lno_533_ins_to_sb()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
with ins as (
    select imp.* from stage_1956_builder_sn sb
    full join tmp.lno_533_valid imp on sb.oth_80154_external_id = imp.external_id
    where sb.oth_80154_external_id is null
)
insert into stage_1956_builder_sn (
         oth_80154_external_id
        ,oth_80153_address
        ,oth_80152_city
        ,oth_80151_omsu_id
        ,ro_23562_a_23562 -- Наименование ОМСУ
        ,ro_23563_a_23563 -- Наименование города
        ,ro_23564_a_23564 -- Номер пункта питания (трансформаторной подстанции)
        ,ro_23567_a_23567 -- Адрес пункта питания
        ,ro_23570_a_23570 -- Фото ПП с внешней стороны
        ,ro_23573_a_23573 -- Фото ПП изнутри с оборудованием
        ,created_at
        ,updated_at
        ,search_name1
        ,name
        ,author_id
        ,stage_id
        ,actuality
        ,visible_id
        ,location
        ,company_id
        ,actor_id
        ,layer_id
        ,first_company_id
        ,second_company_id
        ,third_company_id)
select
         external_id
        ,address
        ,sity
        ,omsu_id
        ,omsu_id
        ,sity
        ,pp_id
        ,address
        ,gallery_id
        ,gallery_id
        ,now()
        ,now()
        ,search_name
        ,name
        ,author_id
        ,stage_id
        ,actuality
        ,visible_id
        ,location
        ,company_id
        ,actor_id
        ,layer_id
        ,company_id
        ,com_2_id
        ,com_3_id
from ins
;
$function$
;
------------------------------------------------------------------------------------------------------------------------
-- tmp.lno_1015_ins_to_sb()
------------------------------------------------------------------------------------------------------------------------
--
--
;
-- drop function tmp.lno_1015_ins_to_sb()
;
CREATE OR REPLACE FUNCTION tmp.lno_1015_ins_to_sb()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
with ins as (
    select imp.* from stage_2011_builder_sn sb
    full join tmp.lno_1015_valid imp on sb.oth_80154_external_id = imp.external_id
    where sb.oth_80154_external_id is null
)
insert into stage_2011_builder_sn (
         oth_80151_omsu_id	        -- ОМСУ
        ,oth_80152_city	            -- Город
        ,oth_80153_address	        -- Адрес
        ,oth_80154_external_id	    -- external_id
        ,oth_80589_layer_id	        -- Номер слоя

        ,oth_80590_first_company_id	    -- first_company_id
        ,oth_80591_second_company_id	-- second_company_id
        ,oth_80592_third_company_id	    -- third_company_id

        ,ro_25424_a_25424	 -- Фото объекта в ночное время (для объектов оснащенных АХП)
        ,ro_25421_a_25421	 -- Фото объекта АХП в дневное время
        ,ro_25418_a_25418	 -- Установленная мощность (если балансодержатель АХП – муниципалитет)
        ,ro_25415_a_25415	 -- Количество светильников (если балансодержатель АХП – муниципалитет)
        ,ro_25412_a_25412	 -- Номер прибора учёта (если балансодержатель АХП – муниципалитет)
        ,ro_25409_a_25409	 -- Координаты шкафа управления (если балансодержатель АХП – муниципалитет)
        ,ro_25406_a_25406	 -- Номер шкафа управления (если балансодержатель АХП – муниципалитет)
        ,ro_25403_a_25403	 -- Номер пункта питания (если балансодержатель АХП – муниципалитет)
        ,ro_25400_a_25400	 -- Наличие системы АХП
        ,ro_25397_a_25397	 -- Тип объекта АХП
        ,ro_25394_a_25394	 -- Название объекта
        ,ro_25391_a_25391	 -- Адрес объекта
        ,ro_25390_a_25390	 -- Координаты
        ,ro_25389_a_25389	 -- Населенный пункт
        ,ro_25388_a_25388	 -- Наименование города
        ,ro_25387_a_25387	 -- Наименование ОМСУ

        ,created_at
        ,updated_at
        ,search_name1
        ,name
        ,author_id
        ,stage_id
        ,actuality
        ,visible_id
        ,location
        ,company_id
        ,actor_id
        )
select
         omsu_id
        ,sity
        ,address
        ,external_id
        ,layer_id

        ,company_id
        ,com_2_id
        ,com_3_id

        ,gallery_id
        ,gallery_id
        ,power            -- Установленная мощность (если балансодержатель АХП – муниципалитет)
        ,lights_count     -- Количество светильников (если балансодержатель АХП – муниципалитет)
        ,accounting_id    -- Номер прибора учёта (если балансодержатель АХП – муниципалитет)
        ,ahp_coord_geo    -- Координаты шкафа управления  (если балансодержатель АХП – муниципалитет)
        ,ahp_id           -- Номер шкафа управления (если балансодержатель АХП – муниципалитет)
        ,pp_id            -- Номер пункта питания (если балансодержатель АХП – муниципалитет)
        ,ahp_availability -- Наличие системы АХП
        ,obj_type         -- Тип объекта АХП
        ,obj_name         -- Название объекта
        ,address
        ,geo
        ,locality
        ,sity
        ,omsu_id

        ,now()
        ,now()
        ,search_name
        ,name
        ,author_id
        ,stage_id
        ,actuality
        ,visible_id
        ,location
        ,company_id
        ,actor_id
from ins
;
$function$
;
------------------------------------------------------------------------------------------------------------------------
-- tmp.lno_493_ins_to_sb()
------------------------------------------------------------------------------------------------------------------------
--
--
;
;
CREATE OR REPLACE FUNCTION tmp.lno_493_ins_to_sb()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
with ins as (
    select imp.* from stage_2005_builder_sn sb
    full join tmp.lno_493_valid imp on sb.oth_80154_external_id = imp.external_id
    where sb.oth_80154_external_id is null
)
insert into stage_2005_builder_sn (
         oth_80151_omsu_id	        -- ОМСУ
        ,oth_80152_city	            -- Город
        ,oth_80153_address	        -- Адрес
        ,oth_80154_external_id	    -- external_id
        ,oth_80589_layer_id	        -- Номер слоя

        ,oth_80590_first_company_id	    -- first_company_id
        ,oth_80591_second_company_id	-- second_company_id
        ,oth_80592_third_company_id	    -- third_company_id

        ,ro_24805_a_24805	-- Наименование ОМСУ
        ,ro_24806_a_24806	-- Наименование города
        ,ro_24807_a_24807	-- Населенный пункт
        ,ro_24808_a_24808	-- Марка опоры
        ,ro_24811_a_24811	-- Материал опоры
        ,ro_24814_a_24814	-- Высота опоры освещения
        ,ro_24817_a_24817	-- Аварийность опоры
        ,ro_24820_a_24820	-- "Фото опоры освещения (опора кронштейн светильник)"
        ,ro_24823_a_24823	-- Фото аварийного состояния опоры освещения
        ,ro_24826_a_24826	-- Длина тросового подвеса
        ,ro_24829_a_24829	-- Высота тросового подвеса
        ,ro_24832_a_24832	-- Количество кронштейнов
        ,ro_24835_a_24835	-- Тип кронштейна
        ,ro_24838_a_24838	-- Высота кронштейна
        ,ro_24841_a_24841	-- Вылет кронштейна
        ,ro_24844_a_24844	-- Аварийность кронштейна
        ,ro_24847_a_24847	-- Количество светильников
        ,ro_24850_a_24850	-- Тип лампы
        ,ro_24853_a_24853	-- Аварийность светильника
        ,ro_24856_a_24856	-- Способ питания светильника
        ,ro_24859_a_24859	-- Наличие дополнительного оборудования опоры освещения
        ,ro_24862_a_24862	-- Наличие дополнительного оборудования опоры освещения 2
        ,ro_24865_a_24865	-- Наличие дополнительного оборудования опоры освещения 3
        ,ro_24868_a_24868	-- Наличие дополнительного оборудования опоры освещения 4

        ,created_at
        ,updated_at
        ,search_name1
        ,name
        ,author_id
        ,stage_id
        ,actuality
        ,visible_id
        ,location
        ,company_id
        ,actor_id
        )
select
 omsu_id
,sity
,'не_предоставляется'
,external_id
,layer_id

,company_id
,com_2_id
,com_3_id

,omsu_id             -- Муниципальное образование
,sity                -- Город
,locality            -- Населенный пункт
,support_mark        -- Марка опоры
,support_material    -- Материал опоры
,support_height      -- Высота опоры освещения
,support_breakdown   -- Аварийность опоры
,gallery_id          -- Фото опоры освещения (опора кронштейн светильник)
,gallery_id          -- Фото аварийного состояния опоры освещения
,cable_length        -- Длина тросового подвеса
,cable_height        -- Высота тросового подвеса
,support_bracket_num -- Количество кронштейнов
,'нет_данных'        -- Тип кронштейна
,'нет_данных'        -- Высота кронштейна
,'нет_данных'        -- Вылет кронштейна
,'нет_данных'        -- Аварийность кронштейна
,support_light_num   -- Количество светильников на опоре (заполняется в отдельном блоке, не удалено специально)
,light_type          -- Тип лампы (заполняется в отдельном блоке, не удалено специально)
,'нет_данных'        -- Аварийность светильника
,light_feed_type     -- Способ питания светильника (заполняется в отдельном блоке, не удалено специально)
,support_add_eq_1    -- Наличие дополнительного оборудования 1
,support_add_eq_2    -- Наличие дополнительного оборудования 2
,support_add_eq_3    -- Наличие дополнительного оборудования 3
,support_add_eq_4    -- Наличие дополнительного оборудования 4

,now()
,now()
,search_name
,name
,author_id
,stage_id
,actuality
,visible_id
,location
,company_id
,actor_id
from ins
;
$function$
;
------------------------------------------------------------------------------------------------------------------------
-- tmp.lno_495_ins_to_sb()
------------------------------------------------------------------------------------------------------------------------
--
--
;
CREATE OR REPLACE FUNCTION tmp.lno_495_ins_to_sb()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
AS
$function$
with ins as (
    select imp.* from stage_1959_builder_sn sb
    full join tmp.lno_495_valid imp on sb.oth_80154_external_id = imp.external_id
    where sb.oth_80154_external_id is null
)
insert into stage_1959_builder_sn (
         oth_80151_omsu_id	        -- ОМСУ
        ,oth_80152_city	            -- Город
        ,oth_80153_address	        -- Адрес
        ,oth_80154_external_id	    -- external_id
        ,oth_80589_layer_id	        -- Номер слоя

        ,oth_80590_first_company_id	    -- first_company_id
        ,oth_80591_second_company_id	-- second_company_id
        ,oth_80592_third_company_id	    -- third_company_id

        ,ro_23635_a_23635	 -- Наименование ОМСУ
        ,ro_23636_a_23636	 -- Наименование города
        ,ro_23637_a_23637	 -- Населенный пункт
        ,ro_23639_a_23639	 -- Номер пункта питания (трансформаторной подстанции)
        ,ro_23642_a_23642	 -- Адреса, по которым проходит линия наружного освещения
        ,ro_23645_a_23645	 -- Фото начала ЛНО и связи с ПП
        ,ro_23648_a_23648	 -- Номер / название / маркировка Шкафа управления
        ,ro_23654_a_23654	 -- Фото шкафа управления с оборудованием
        ,ro_23657_a_23657	 -- Наличие АСУНО в ШУ
        ,ro_23660_a_23660	 -- Фото модуля управления АСУНО
        ,ro_23663_a_23663	 -- Марка прибора учета электроэнергии
        ,ro_23666_a_23666	 -- Номер ПУ
        ,ro_23669_a_23669	 -- Фото прибора учёта
        ,ro_23672_a_23672	 -- Количество опор освещения
        ,ro_23675_a_23675	 -- Количество светильников

        ,created_at
        ,updated_at
        ,search_name1
        ,name
        ,author_id
        ,stage_id
        ,actuality
        ,visible_id
        ,location
        ,company_id
        ,actor_id
        )
select
 omsu_id
,sity
,address
,external_id
,layer_id

,company_id
,com_2_id
,com_3_id

,omsu_id             -- Муниципальное образование
,sity                -- Город
,locality            -- Населенный пункт
,pp_id               -- Номер пункта питания (трансформаторной подстанции)
,lno_address         -- Адреса, по которым проходит линия наружного освещения
,gallery_id          -- Фото начала ЛНО и связи с ПП
,c_cab_info          -- Номер / название / маркировка Шкафа управления
,gallery_id          -- Фото шкафа управления с оборудованием
,asuno               -- Наличие АСУНО в ШУ
,gallery_id          -- Фото модуля управления АСУНО
,accounting_mark     -- Марка прибора учета электроэнергии
,pu_id               -- Номер ПУ
,gallery_id          -- Фото прибора учёта
,support_num         -- Количество опор освещения
,lamp_num            -- Количество светильников

,now()
,now()
,search_name
,name
,author_id
,stage_id
,actuality
,visible_id
,location
,company_id
,actor_id
from ins
;
$function$
;
select * from tmp.lno_1015_valid;