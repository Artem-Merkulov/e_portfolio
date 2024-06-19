-- CTE 1 
-- Таблицы, которая поможет с анализом доли пользователей, которые активны в группах (user_group_messages) — пишут туда что-то
with user_group_messages as (
                        SELECT 
                               lum.hk_user_id,
                               lum.hk_message_id,
                               lgd.hk_group_id       AS hk_group_id,                            -- Хэш ключи каждой уникальной группы   
                               COUNT (DISTINCT lum.hk_user_id)  AS cnt_users_in_group_with_messages        -- Количество уникальных пользователей в группе, которые написали хотя бы раз
                        from   STV202310168__DWH.l_user_message as lum
                        left join STV202310168__DWH.l_groups_dialogs as lgd on lum.hk_message_id = lgd.hk_message_id
                        group by lum.hk_user_id,
                                 lum.hk_message_id,
                                 lgd.hk_group_id
                        ORDER BY cnt_users_in_group_with_messages DESC
)
select hk_group_id,
       cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10; 


-- CTE 2  
-- Таблицу, из которой можно получать количество пользователей, вступивших в группы
WITH user_group_log AS ( SELECT
                                sah.hk_l_user_group_activity,
                                sah.event,
	                            luga.hk_user_id,
	                            luga.hk_group_id AS hk_group_id,                                    -- Хэш-ключ каждой группы
	                            COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users                         -- Количество пользователей в группе, которые в неё просто вступили
                         FROM
	                            STV202310168__DWH.s_auth_history AS sah
                         LEFT JOIN STV202310168__DWH.l_user_group_activity luga
                                                                    ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
	                     LEFT JOIN STV202310168__DWH.h_groups AS hg 
	                                                                ON luga.hk_group_id = hg.hk_group_id 
	                     WHERE event = 'add'     AND hg.hk_group_id IN ( SELECT                            -- 10 самых ранних групп
		                                                                        hk_group_id
	                                                                     FROM
		                                                                        STV202310168__DWH.h_groups
	                                                                     ORDER BY
		                                                                        registration_dt
	                                                                     LIMIT 10 )
	                    GROUP BY sah.hk_l_user_group_activity,
                                 sah.event,
	                             luga.hk_user_id,
	                             luga.hk_group_id )
SELECT
	hk_group_id,
	cnt_added_users
FROM
	user_group_log 
ORDER BY
	cnt_added_users
LIMIT 10;


-- Ответ бизнесу
with user_group_log as ( SELECT
                                sah.hk_l_user_group_activity,
                                sah.event,
	                            luga.hk_user_id,
	                            luga.hk_group_id AS hk_group_id,                                    -- Хэш-ключ каждой группы
	                            COUNT(DISTINCT luga.hk_user_id) AS cnt_added_users                         -- Количество пользователей в группе, которые в неё просто вступили
                         FROM
	                            STV202310168__DWH.s_auth_history AS sah
                         LEFT JOIN STV202310168__DWH.l_user_group_activity luga
                                                                    ON luga.hk_l_user_group_activity = sah.hk_l_user_group_activity
	                     LEFT JOIN STV202310168__DWH.h_groups AS hg 
	                                                                ON luga.hk_group_id = hg.hk_group_id 
	                     WHERE event = 'add'     AND hg.hk_group_id IN ( SELECT                            -- 10 самых ранних групп
		                                                                        hk_group_id
	                                                                     FROM
		                                                                        STV202310168__DWH.h_groups
	                                                                     ORDER BY
		                                                                        registration_dt
	                                                                     LIMIT 10 )
	                    GROUP BY sah.hk_l_user_group_activity,
                                 sah.event,
	                             luga.hk_user_id,
	                             luga.hk_group_id ),
user_group_messages as (SELECT 
                               lum.hk_user_id,
                               lum.hk_message_id,
                               lgd.hk_group_id       AS hk_group_id,                            -- Хэш ключи каждой уникальной группы   
                               COUNT (DISTINCT lum.hk_user_id)  AS cnt_users_in_group_with_messages        -- Количество уникальных пользователей в группе, которые написали хотя бы раз
                        from   STV202310168__DWH.l_user_message as lum
                        left join STV202310168__DWH.l_groups_dialogs as lgd on lum.hk_message_id = lgd.hk_message_id
                        group by lum.hk_user_id,
                                 lum.hk_message_id,
                                 lgd.hk_group_id
                        ORDER BY cnt_users_in_group_with_messages DESC)
SELECT
	ugl.hk_group_id AS hk_group_id,	                                                                       -- Хэш-ключ группы
	ugl.cnt_added_users AS cnt_added_users,                                                                -- Количество новых пользователей группы (event = add)
	ugm.cnt_users_in_group_with_messages AS cnt_users_in_group_with_messages,                              -- Количество пользователей группы, которые написали хотя бы одно сообщение 
	(ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users * 100)     AS group_conversion             -- CR Количество написавших сообщение к количеству добавленных в группу                  
FROM 
	user_group_log AS ugl
LEFT JOIN user_group_messages AS ugm ON
	ugl.hk_group_id = ugm.hk_group_id
ORDER BY
	ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users DESC;
