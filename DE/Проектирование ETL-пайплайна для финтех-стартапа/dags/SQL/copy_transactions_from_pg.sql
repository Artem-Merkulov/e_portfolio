COPY STV202310168__STAGING.transactions(operation_id,
                                        account_number_from,
                                        account_number_to,
                                        currency_code,
                                        country,
                                        status,
                                        transaction_type,
                                        amount,
                                        transaction_dt) FROM STDIN DELIMITER ',' ENCLOSED BY '"';