-- 2 Шаг. После загрузки в таблицы данных - установить связи между таблицами.

-- auto_dv.dialog внешние включи
ALTER TABLE auto_dv.dialog ADD CONSTRAINT dialogs_users_message_from_fk FOREIGN KEY (message_from) REFERENCES auto_dv."user"(user_id);
ALTER TABLE auto_dv.dialog ADD CONSTRAINT dialogs_users_message_to_fk FOREIGN KEY (message_to) REFERENCES auto_dv."user"(user_id);

-- auto_dv."group" внешние включи
ALTER TABLE auto_dv."group" ADD CONSTRAINT groups_users_fk FOREIGN KEY (admin_id) REFERENCES auto_dv."user"(user_id);

-- auto_dv.group_log внешние включи
ALTER TABLE auto_dv.group_log ADD CONSTRAINT group_log_groups_fk FOREIGN KEY (group_id) REFERENCES auto_dv."group"(group_id);
ALTER TABLE auto_dv.group_log ADD CONSTRAINT group_log_users_fk FOREIGN KEY (user_id) REFERENCES auto_dv."user"(user_id);