SELECT DISTINCT
                                date_update,
                                currency_code,
                                currency_code_with,
                                currency_with_div
                         FROM public.a_merkulov_currencies
                         WHERE date_update::date = :business_date
                         AND LENGTH(currency_code::varchar) = 3
                         AND LENGTH(currency_code_with::varchar) = 3;