TRUNCATE mart.f_customer_retention CASCADE;

CREATE SEQUENCE d_first_properties_sequence;

INSERT INTO mart.f_customer_retention (
   id,
   item_id,
   period_id,
   new_customers_count,
   returning_customers_count,
   refunded_customers_count,
   new_customers_revenue,
   returning_customers_revenue,
   customers_refunded
)
WITH
       new_customers as
         (select customer_id,
                 payment_amount
          from staging.user_order_log uol 
          where status = 'shipped'
          group by customer_id,
                   payment_amount
          having count(*) = 1),
       returning_customers as
         (select customer_id,
                 payment_amount
          from staging.user_order_log uol
          where status = 'shipped'
          group by customer_id,
                   payment_amount
          having count(*) > 1),
       refunded_customers as
         (select customer_id,
                 payment_amount
          from staging.user_order_log uol
          where status = 'refunded'
          group by customer_id,
                   payment_amount)
SELECT 
       nextval('d_first_properties_sequence')::bigint AS id,
       uol.item_id,
       EXTRACT(WEEK FROM CAST(uol.date_time AS date)) AS period_id,
       COUNT(new_customers.customer_id) AS new_customers_count,
       COUNT(returning_customers.customer_id) AS returning_customers_count,
       COUNT(refunded_customers.customer_id) AS refunded_customers_count,
       SUM(new_customers.payment_amount) AS new_customers_revenue,
       SUM(returning_customers.payment_amount) AS returning_customers_revenue,
       COUNT(refunded_customers.customer_id) AS customers_refunded
FROM 
       new_customers FULL JOIN returning_customers ON new_customers.customer_id = returning_customers.customer_id
                     FULL JOIN refunded_customers ON returning_customers.customer_id = refunded_customers.customer_id
                     FULL JOIN staging.user_order_log uol ON returning_customers.customer_id = uol.customer_id
GROUP BY 
       uol.item_id,
       period_id
ORDER BY 
       period_id ASC;
       
DROP SEQUENCE d_first_properties_sequence; 