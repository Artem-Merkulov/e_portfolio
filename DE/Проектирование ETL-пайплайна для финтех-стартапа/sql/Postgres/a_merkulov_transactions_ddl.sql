-- STV202310168__STAGING.transactions определение
DROP TABLE IF EXISTS public.a_merkulov_transactions;

CREATE TABLE public.a_merkulov_transactions(
    operation_id varchar(60) NULL,
    account_number_from int NULL,
    account_number_to int NULL,
    currency_code int NULL,
    country varchar(30) NULL,
    status varchar(30) NULL,
    transaction_type varchar(30) NULL,
    amount int NULL,
    transaction_dt timestamp NULL,
    CONSTRAINT transactions_pk PRIMARY KEY (operation_id))
