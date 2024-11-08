{{ config(store_failures_as="table", error_if = '>10', warn_if = '!=0' ) }}

(SELECT 
    min(u.registration_dt) as datestamp,
    'earliest user registration' as info
FROM {{ source('automatedv', 'user') }} u)
UNION ALL
(SELECT
    max(u.registration_dt),
    'latest user registration'
FROM {{ source('automatedv', 'user') }} u)
UNION ALL
(SELECT 
    min(g.registration_dt) as datestamp,
    'earliest group creation' as info
FROM {{ source('automatedv', 'group') }} g)
UNION ALL
(SELECT
    max(g.registration_dt),
    'latest group creation'
FROM {{ source('automatedv', 'group') }} g)
UNION ALL
(SELECT 
    min(d.message_ts) as datestamp,
    'earliest dialog message' as info
FROM {{ source('automatedv', 'dialog') }} d)
UNION ALL
(SELECT
    max(d.message_ts),
    'latest dialog message'
FROM {{ source('automatedv', 'dialog') }} d)