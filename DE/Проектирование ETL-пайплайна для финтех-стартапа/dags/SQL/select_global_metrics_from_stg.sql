        SELECT DISTINCT c.date_update::date AS date_update,
                        t.currency_code AS currency_code,
                        ROUND(sum(abs(t.amount) * CASE WHEN t.currency_code = 420 THEN 1 ELSE c.currency_with_div END), 2) AS amount_total,
                        COUNT(t.operation_id) AS cnt_transactions,
                        ROUND(COUNT(*)/COUNT(DISTINCT CASE WHEN RIGHT(t.transaction_type, 8) = 'outgoing' THEN t.account_number_from ELSE t.account_number_to END), 2) AS avg_transactions_per_account,
                        COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
         FROM STV202310168__STAGING.currencies c 
         RIGHT OUTER JOIN STV202310168__STAGING.transactions t ON c.date_update::date = t.transaction_dt::date
         WHERE c.date_update::date = :business_date
         GROUP BY c.date_update,
                  t.currency_code
                  ORDER BY date_update;