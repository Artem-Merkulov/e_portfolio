-- Тут буду описывать ход выполнения проекта:

-- 1. Создаю таблицу shipping в БД de public:

DROP TABLE IF EXISTS public.shipping;

CREATE TABLE public.shipping(
   ID                               serial,
   shipping_id                      BIGINT,
   sale_id                          BIGINT,
   order_id                         BIGINT,
   client_id                        BIGINT,
   payment_amount                   NUMERIC(14,2),
   state_datetime                   TIMESTAMP,
   product_id                       BIGINT,
   description                      text,
   vendor_id                        BIGINT,
   name_category                    text,
   base_country                     text,
   status                           text,
   state                            text,
   shipping_plan_datetime           TIMESTAMP,
   hours_to_plan_shipping           NUMERIC(14,2),
   shipping_transfer_description    text,
   shipping_transfer_rate           NUMERIC(14,3),
   shipping_country                 text,
   shipping_country_base_rate       NUMERIC(14,3),
   vendor_agreement_description     text,
   PRIMARY KEY (ID)
);
CREATE INDEX shipping_id ON public.shipping (shipping_id);
COMMENT ON COLUMN public.shipping.shipping_id is 'id of shipping of sale';

-- 2. Заполняю таблицу данными с помощью импорта в DBeaver из файла shipping.csv. Как SQL скриптом это сделать - не знаю. Не делал ни разу.
-- 3. Проверяю Правильность заполнения таблицы. Вывожу количество строк в SQL редакторе. До этого открывал таблицу в юпитере - количество строк совпало. 
--    Количество столбцов и их наименования - тоже.
-- 4. Создаю справочник стоимости доставки в страны shipping_country_rates из данных, указанных в shipping_country и shipping_country_base_rate:

DROP TABLE IF EXISTS public.shipping_country_rates;

CREATE TABLE public.shipping_country_rates(

   id SERIAL PRIMARY KEY,
   shipping_country TEXT,
   shipping_country_base_rate       NUMERIC(14,3)
);

INSERT INTO public.shipping_country_rates 
            (shipping_country, 
             shipping_country_base_rate)

SELECT 
       DISTINCT shipping_country,
       AVG(shipping_country_base_rate) AS shipping_country_base_rate
FROM public.shipping s
GROUP BY shipping_country;

-- 5. Создаю справочник тарифов доставки вендора по договору shipping_agreement из данных строки vendor_agreement_description:
   
DROP TABLE IF EXISTS public.shipping_agreement;

CREATE TABLE IF NOT EXISTS shipping_agreement  (
    agreement_id Integer PRIMARY KEY,
    agreement_number TEXT,
    agreement_rate Numeric(14,2),
    agreement_commission Numeric(14,2)
);

INSERT INTO shipping_agreement 
   (agreement_id,
    agreement_number,
    agreement_rate,
    agreement_commission)

SELECT DISTINCT
       CAST((regexp_split_to_array(vendor_agreement_description, E':'))[1] AS Integer) AS agreement_id,
            (regexp_split_to_array(vendor_agreement_description, E':'))[2] AS agreement_number,
       CAST((regexp_split_to_array(vendor_agreement_description, E':'))[3] AS Numeric(14,2)) AS agreement_rate,
       CAST((regexp_split_to_array(vendor_agreement_description, E':'))[4] AS Numeric(14,2)) AS agreement_commission
FROM public.shipping s;

-- 6. Создаю справочник о типах доставки shipping_transfer из строки shipping_transfer_description и поля shipping_transfer_rate:

DROP TABLE IF EXISTS public.shipping_transfer;

CREATE TABLE IF NOT EXISTS shipping_transfer  (
    id Serial PRIMARY KEY,
    transfer_type TEXT,
    transfer_model TEXT,
    shipping_transfer_rate Numeric
);

INSERT INTO shipping_transfer 
   (transfer_type,
    transfer_model,
    shipping_transfer_rate)

SELECT DISTINCT        
            (regexp_split_to_array(shipping_transfer_description, E':'))[1] AS transfer_type,
            (regexp_split_to_array(shipping_transfer_description, E':'))[2] AS transfer_model,
        CAST(shipping_transfer_rate AS NUMERIC) AS shipping_transfer_rate
FROM public.shipping s;


-- 7. Создаю таблицу shipping_info, справочник комиссий по странам, с уникальными доставками shipping_id и связываю ее с созданными справочниками shipping_country_rates, shipping_agreement, shipping_transfer и константной информации о доставке shipping_plan_datetime, payment_amount, vendor_id:

DROP TABLE IF EXISTS public.shipping_info;

CREATE TABLE IF NOT EXISTS shipping_info  (
    shipping_id Int PRIMARY KEY,
    vendor_id Int,
    payment_amount Numeric,
    shipping_plan_datetime Timestamp,
    shipping_transfer_id Int,
    shipping_agreement_id Int,
    shipping_country_rate_id Int,
    FOREIGN KEY (shipping_transfer_id) REFERENCES shipping_transfer(id),
    FOREIGN KEY (shipping_agreement_id) REFERENCES shipping_agreement(agreement_id),
    FOREIGN KEY (shipping_country_rate_id) REFERENCES shipping_country_rates(id)
);

INSERT INTO shipping_info 
   (shipping_id, 
    vendor_id, 
    payment_amount, 
    shipping_plan_datetime, 
    shipping_transfer_id, 
    shipping_agreement_id, 
    shipping_country_rate_id)

SELECT 
    DISTINCT s.shipping_id AS shipping_id,
    s.vendor_id AS vendor_id,
    s.payment_amount,
    s.shipping_plan_datetime,
    st.id AS shipping_transfer_id, 
    sa.agreement_id,
    scr.id AS shipping_country_rate_id 
FROM public.shipping s
    JOIN public.shipping_transfer st ON (REGEXP_SPLIT_TO_ARRAY(s.shipping_transfer_description, E'\\:+'))[1] = st.transfer_type
    AND (REGEXP_SPLIT_TO_ARRAY(s.shipping_transfer_description, E'\\:+'))[2] = st.transfer_model
    JOIN public.shipping_country_rates scr ON s.shipping_country = scr.shipping_country    
    JOIN public.shipping_agreement sa ON CAST((REGEXP_SPLIT_TO_ARRAY(s.vendor_agreement_description, E'\\:+'))[1] AS int8) = sa.agreement_id;

-- 8. Создаю таблицу статусов о доставке shipping_status:

DROP TABLE IF EXISTS public.shipping_status;

CREATE TABLE IF NOT EXISTS shipping_status (
    shipping_id Int PRIMARY KEY,
    status Text,
    state Text,
    shipping_start_fact_datetime Timestamp,
    shipping_end_fact_datetime Timestamp
);

INSERT INTO shipping_status 
   (shipping_id,
    status,
    state,
    shipping_start_fact_datetime,
    shipping_end_fact_datetime)

WITH shipping_agregates AS (SELECT 
                                   shipping_id,
                                   MAX(state_datetime) state_datetime,
                                   MAX(CASE WHEN state = 'booked' THEN state_datetime END) shipping_start_fact_datetime,
                                   MAX(CASE WHEN state ='recieved' THEN state_datetime END) shipping_end_fact_datetime
                               FROM public.shipping s 
                               GROUP BY shipping_id)
SELECT 
       sa.shipping_id,
       status,
       state,
       shipping_start_fact_datetime,
	   shipping_end_fact_datetime
FROM public.shipping s 
JOIN shipping_agregates sa ON s.shipping_id = sa.shipping_id
AND s.state_datetime = sa.state_datetime;

-- 9. Создаю представление shipping_datamart на основании готовых таблиц для аналитики:

CREATE OR REPLACE VIEW shipping_datamart AS (
SELECT
       si.shipping_id,
       si.vendor_id,
       st.transfer_type,
       (ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime) full_day_at_shipping,
       (CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN 1 ELSE 0 END) is_delay,
       (CASE WHEN ss.status LIKE '%finish%' THEN 1 ELSE 0 END) is_shipping_finish,
       (CASE WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime 
        THEN DATE_PART('day', AGE(ss.shipping_end_fact_datetime, si.shipping_plan_datetime))
        ELSE 0 END) delay_day_at_shipping,
       si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) vat,
       (si.payment_amount * sa.agreement_commission) profit
FROM public.shipping_info si 
JOIN shipping_status ss ON si.shipping_id = ss.shipping_id
JOIN shipping_transfer st ON si.shipping_transfer_id = st.id
JOIN shipping_agreement sa ON si.shipping_agreement_id = sa.agreement_id
JOIN shipping_country_rates scr ON si.shipping_country_rate_id = scr.id 
);

