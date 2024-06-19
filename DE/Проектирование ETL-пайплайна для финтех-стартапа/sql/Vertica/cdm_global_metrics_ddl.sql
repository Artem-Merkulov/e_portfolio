DROP TABLE IF EXISTS STV202310168.global_metrics;

CREATE TABLE STV202310168.global_metrics
(
    date_update timestamp NOT NULL, -- дата расчёта
    currency_from int NOT NULL, -- код валюты транзакции
    amount_total float NOT NULL, -- общая сумма транзакций по валюте в долларах
    cnt_transactions int NOT NULL, -- общий объём транзакций по валюте
    avg_transactions_per_account float NOT NULL, -- средний объём транзакций с аккаунта
    cnt_accounts_make_transactions bigint NOT NULL -- количество уникальных аккаунтов с совершёнными транзакциями по валюте
)
ORDER BY date_update
SEGMENTED BY HASH(date_update, currency_from) ALL NODES;

COMMENT ON TABLE STV202310168.global_metrics IS 'Витрина global_metrics с агрегацией по дням. 
Витрина помогает отвечать на такие вопросы:
- какая ежедневная динамика сумм переводов в разных валютах;
- какое среднее количество транзакций на пользователя;
- какое количество уникальных пользователей совершают транзакции в валютах;
- какой общий оборот компании в единой валюте.';