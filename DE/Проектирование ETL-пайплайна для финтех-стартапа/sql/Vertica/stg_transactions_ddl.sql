-- STV202310168__STAGING.transactions определение
DROP TABLE IF EXISTS STV202310168__STAGING.transactions;

CREATE TABLE STV202310168__STAGING.transactions(
    operation_id uuid NULL,
    account_number_from int NULL,
    account_number_to int NULL,
    currency_code int NULL,
    country varchar(30) NULL,
    status varchar(30) NULL,
    transaction_type varchar(30) NULL,
    amount int NULL,
    transaction_dt timestamp NULL,
    CONSTRAINT transactions_pk PRIMARY KEY (operation_id),
    CONSTRAINT check_account_number_to_length CHECK ((LENGTH(account_number_to::varchar) <= 7)
                                                  AND (LENGTH(account_number_to::varchar) > 0)),
    CONSTRAINT check_account_number_from_length CHECK ((LENGTH(account_number_from::varchar) <= 7)
                                                  AND (LENGTH(account_number_to::varchar) > 0)),
    CONSTRAINT check_country_length CHECK ((LENGTH(country::varchar) <= 8)
                                       AND (LENGTH(country::varchar) >= 3)),                                              
    CONSTRAINT check_currency_code_length CHECK (LENGTH(currency_code::varchar) = 3),
    CONSTRAINT check_status_values CHECK (status IN ('queued', 
                                                     'in_progress', 
                                                     'done', 
                                                     'chargeback', 
                                                     'blocked')),
    CONSTRAINT check_object_type_values CHECK (transaction_type IN ('authorization', 
                                                                    'sbp_incoming', 
                                                                    'sbp_outgoing', 
                                                                    'transfer_incoming', 
                                                                    'transfer_outgoing', 
                                                                    'c2b_partner_incoming', 
                                                                    'c2b_partner_outgoing',
                                                                    'loyalty_cashback',
                                                                    'authorization_commission')),
    CONSTRAINT check_transaction_dt_range CHECK (transaction_dt >= '2022-10-01' AND transaction_dt < '2022-11-01')
)
ORDER BY transaction_dt
SEGMENTED BY HASH(operation_id, transaction_dt) ALL NODES;

COMMENT ON TABLE STV202310168__STAGING.transactions IS 'Transaction list';

