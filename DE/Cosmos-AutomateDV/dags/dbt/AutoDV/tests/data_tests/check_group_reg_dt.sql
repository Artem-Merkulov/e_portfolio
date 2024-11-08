{{ config(store_failures_as="table", error_if = '>10', warn_if = '!=0' ) }}

SELECT * FROM {{ source('automatedv', 'group') }} u 
WHERE registration_dt::DATE < '2004-01-01'::DATE OR registration_dt::DATE > NOW()::DATE