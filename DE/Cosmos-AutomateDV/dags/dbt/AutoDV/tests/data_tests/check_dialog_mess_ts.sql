{{ config(store_failures_as="table", error_if = '>10', warn_if = '!=0' ) }}

SELECT * FROM {{ source('automatedv', 'dialog') }} u 
WHERE message_ts::DATE < '2004-01-01'::DATE OR message_ts::DATE > NOW()::DATE