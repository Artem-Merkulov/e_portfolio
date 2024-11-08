{{ config(store_failures_as="table", error_if = '>4', warn_if = '!=0' ) }}

SELECT
	'user' AS dataset,
	COUNT(user_id) total,
	COUNT(DISTINCT user_id) uniq
FROM 
    {{ source('automatedv', 'user') }} u
UNION ALL
SELECT
	'group' AS dataset,
	COUNT(group_id) total,
	COUNT(DISTINCT group_id) uniq 
FROM
	{{ source('automatedv', 'group') }} g
UNION ALL
SELECT
	'dialog' AS dataset,
	COUNT(message_id) total,
	COUNT(DISTINCT message_id) uniq
FROM
	{{ source('automatedv', 'dialog') }}
UNION ALL
SELECT
	'group_log' AS dataset,
	COUNT(group_id) total,
	COUNT(DISTINCT group_id) uniq
FROM
	{{ source('automatedv', 'group_log') }} gl