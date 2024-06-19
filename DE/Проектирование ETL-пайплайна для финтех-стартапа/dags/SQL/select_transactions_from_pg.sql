SELECT
	DISTINCT 
    operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt
FROM
	public.a_merkulov_transactions
WHERE
	transaction_dt::date = :business_date
	AND (account_number_to::int > 0)
	AND (account_number_to::int < 1000000)
	AND (account_number_from::int > 0)
	AND (account_number_from::int < 1000000)
	AND (LENGTH(account_number_to::varchar) <= 7)
	AND (LENGTH(account_number_to::varchar) > 0)
	AND (LENGTH(account_number_from::varchar) <= 7)
	AND (LENGTH(account_number_to::varchar) > 0)
	AND (LENGTH(country::varchar) <= 8)
	AND (LENGTH(country::varchar) >= 3)
	AND LENGTH(currency_code::varchar) = 3
	AND status IN ('queued', 
                                          'in_progress', 
                                          'done', 
                                          'chargeback', 
                                          'blocked')
	AND transaction_type IN ('authorization', 
                                                    'sbp_incoming', 
                                                    'sbp_outgoing', 
                                                    'transfer_incoming', 
                                                    'transfer_outgoing', 
                                                    'c2b_partner_incoming', 
                                                    'c2b_partner_outgoing',
                                                    'loyalty_cashback',
                                                    'authorization_commission')
	AND transaction_dt >= '2022-10-01'
	AND transaction_dt < '2022-11-01'