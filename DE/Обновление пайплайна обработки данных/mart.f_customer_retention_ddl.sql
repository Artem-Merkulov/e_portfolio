DROP TABLE IF EXISTS mart.f_customer_retention;

CREATE TABLE mart.f_customer_retention (
   id bigint,
   item_id int4,
-- идентификатор категории товара.
period_name varchar DEFAULT 'Weekly',
-- weekly.
period_id NUMERIC,
-- идентификатор периода (номер недели или номер месяца).
new_customers_count int8,
-- кол-во новых клиентов (тех, которые сделали только один заказ за рассматриваемый промежуток времени).
returning_customers_count int8,
-- кол-во вернувшихся клиентов (тех, которые сделали только несколько заказов за рассматриваемый промежуток времени).
refunded_customers_count int8,
-- кол-во клиентов, оформивших возврат за рассматриваемый промежуток времени.
new_customers_revenue NUMERIC(10,
2),
-- доход с новых клиентов.
returning_customers_revenue NUMERIC(10,
2),
-- доход с вернувшихся клиентов.
customers_refunded int8,
-- количество возвратов клиентов.
   CONSTRAINT f_customer_retention_table_pkey PRIMARY KEY (id)
);