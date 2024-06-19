-- DDL создания deliverysystem_restaurants:

CREATE TABLE stg.deliverysystem_restaurants (
	id serial4 NOT NULL, -- суррогатный ключ
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	CONSTRAINT deliverysystem_restaurants_pkey PRIMARY KEY (id) -- Даты updat_ys тут никакой нет, поэтому поля с датой не добавляю. Не уверен, что это верно.
);

-- DDL создания deliverysystem_couriers:

CREATE TABLE stg.deliverysystem_couriers (
	id serial4 NOT NULL, -- сурргогатный ключ
        object_id varchar NOT NULL,
        object_value text NOT NULL, -- JSON как есть
	CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY (id) -- Даты updat_ys тут никакой нет, поэтому поля с датой не добавляю. Не уверен, что это верно. 
);

-- DDL создания deliverysystem_deliveries:

CREATE TABLE stg.deliverysystem_deliveries (
    id serial4 NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL, -- тут будет JSON как есть
	delivery_ts timestamp NOT NULL, -- не уверен, что тут должно быть delivery_ts, но update ts - отсутствует и надо наверно взять какую-то дату... 
	CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY (id)
);