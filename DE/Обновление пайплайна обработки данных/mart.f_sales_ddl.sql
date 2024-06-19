DROP TABLE IF EXISTS mart.sales;

CREATE TABLE mart.sales (
     date_id INT4,
     item_id INT4,
     city_id INT4, 
     revenue NUMERIC(10, 2), 
     quantity INT8,
     CONSTRAINT f_sales_table_pkey PRIMARY KEY (date_id));