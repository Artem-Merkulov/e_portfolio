{{ config(store_failures_as="table", error_if = '>10', warn_if = '!=0' ) }}

SELECT * FROM {{ source('automatedv', 'user') }}
WHERE age < 0 or age > 100