{{ config(store_failures_as="table", error_if = '>4', warn_if = '!=0' ) }}

(SELECT  'missing group admin info' as info, count(1)
FROM {{ source('automatedv', 'group') }} g
WHERE g.admin_id IS NULL)
UNION ALL
(SELECT  'missing sender info', COUNT(1)
FROM {{ source('automatedv', 'dialog') }} d
WHERE d.message_from IS NULL)
UNION ALL
(SELECT 'missing receiver info', COUNT(1)
FROM {{ source('automatedv', 'dialog') }} d
WHERE d.message_to IS NULL)
UNION ALL 
(SELECT 'norm receiver info', COUNT(1)
FROM {{ source('automatedv', 'dialog') }} d
WHERE d.message_to IS NOT NULL)