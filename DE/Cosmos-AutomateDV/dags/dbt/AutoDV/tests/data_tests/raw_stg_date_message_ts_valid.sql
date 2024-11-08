SELECT * FROM {{ ref('raw_stg') }}
WHERE message_ts::DATE > NOW() OR message_ts::DATE < '2004-01-01'::DATE