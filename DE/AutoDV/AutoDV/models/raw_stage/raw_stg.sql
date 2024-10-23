{{ config( materialized='view' ) }}

SELECT 
	d.message_id AS message_id,
    d.message_ts AS message_ts,
    d.message_from AS message_from,
    d.message_to AS message_to,
    d.message AS "message",
    d.message_group AS message_group,
	u.user_id AS user_id,
	u.chat_name AS chat_name,
	u.registration_dt AS user_reg_dt,
	u.country AS country,
	u."age" AS age,
	g.group_id AS group_id,
	g.admin_id AS admin_id,
	g.group_name AS group_name,
	g.registration_dt AS group_reg_dt,
	g.is_private AS is_private,
    gl.group_id AS gl_group_id,
    gl.user_id AS gl_user_id,
    gl.user_id_from AS user_id_from,
    gl."event" AS "event",
    gl.datetime AS gl_datetime
FROM     
    {{ source('automatedv', 'dialog') }} AS d
LEFT JOIN 
    {{ source('automatedv', 'user') }} AS u
	ON d.message_from = u.user_id OR d.message_to = u.user_id 
LEFT JOIN   
	{{ source('automatedv', 'group') }} AS g
	ON u.user_id = g.admin_id 
LEFT JOIN 
	{{ source('automatedv', 'group_log') }} gl
	ON u.user_id = gl.user_id
WHERE 
	d.message_ts::DATE = '{{ var('load_dt') }}'