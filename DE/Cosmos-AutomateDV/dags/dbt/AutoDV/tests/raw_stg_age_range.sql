SELECT * FROM {{ ref('raw_stg') }}
WHERE age < 0 or age > 100