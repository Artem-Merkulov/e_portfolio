-- 1 Шаг. Создание таблиц - источников. Далее нужно загрузить в таблицы данные.

DROP TABLE IF EXISTS automate_dv."user" CASCADE;
DROP TABLE IF EXISTS automate_dv.dialog CASCADE;
DROP TABLE IF EXISTS automate_dv."group" CASCADE;
DROP TABLE IF EXISTS automate_dv.group_log CASCADE;

CREATE TABLE automate_dv.dialog (
	message_id int4 NOT NULL,
	message_ts timestamp NULL,
	message_from int4 NULL,
	message_to int4 NULL,
	message varchar(1000) NULL,
	message_group float8 NULL,
	CONSTRAINT dialogs_pk PRIMARY KEY (message_id)
);

CREATE TABLE automate_dv."user" (
	user_id int4 NOT NULL,
	chat_name varchar(200) NULL,
	registration_dt timestamp NULL,
	country varchar(200) NULL,
	age int4 NULL,
	CONSTRAINT users_pk PRIMARY KEY (user_id)
);

CREATE TABLE automate_dv."group" (
	group_id int4 NOT NULL,
	admin_id int4 NULL,
	group_name varchar(100) NULL,
	registration_dt timestamp NULL,
	is_private int4 NULL,
	CONSTRAINT groups_pk PRIMARY KEY (group_id)
);

CREATE TABLE automate_dv.group_log (
	group_id int4 NULL,
	user_id int4 NULL,
	user_id_from float8 NULL,
	"event" varchar(100) NULL,
	datetime timestamp NULL
);