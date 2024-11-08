{{ config(store_failures_as="table", error_if = '>10', warn_if = '!=0' ) }}

SELECT g.group_name
FROM {{ source('automatedv', 'group') }} AS g 
LEFT JOIN {{ source('automatedv', 'user') }} AS u 
ON g.admin_id = u.user_id
WHERE g.admin_id IS NULL