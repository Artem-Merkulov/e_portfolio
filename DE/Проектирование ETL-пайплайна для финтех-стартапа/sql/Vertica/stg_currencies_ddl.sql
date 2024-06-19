-- STV202310168__STAGING.currencies определение
DROP TABLE IF EXISTS STV202310168__STAGING.currencies;

CREATE TABLE STV202310168__STAGING.currencies
(
    date_update timestamp NOT NULL,
    currency_code int NOT NULL,
    currency_code_with int NOT NULL,
    currency_with_div numeric(5, 3) NOT NULL,
    CONSTRAINT ucCodes UNIQUE (date_update, 
                               currency_code, 
                               currency_code_with,
                               currency_with_div),
    CONSTRAINT check_date_update_range CHECK (date_update >= '2022-10-01' AND date_update < '2022-11-01'),
    CONSTRAINT check_currency_code_length CHECK (LENGTH(currency_code::varchar) = 3),
    CONSTRAINT check_currency_code_with_length CHECK (LENGTH(currency_code_with::varchar) = 3)
)
ORDER BY date_update
SEGMENTED BY HASH(date_update) ALL NODES;

COMMENT ON TABLE STV202310168__STAGING.currencies IS 'Currency exchange rates';