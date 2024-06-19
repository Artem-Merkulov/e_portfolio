DROP TABLE IF EXISTS public.a_merkulov_currencies;

CREATE TABLE public.a_merkulov_currencies
(
    date_update timestamp NOT NULL,
    currency_code int NOT NULL,
    currency_code_with int NOT NULL,
    currency_with_div numeric(5, 3) NOT NULL,
    CONSTRAINT ucCodes UNIQUE (date_update, 
                               currency_code, 
                               currency_code_with,
                               currency_with_div)
    )