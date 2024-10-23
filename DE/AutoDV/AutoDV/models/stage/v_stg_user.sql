{{ config( materialized='view' ) }}

{% set yaml_metadata %}
source_model: raw_stg
derived_columns: 
    load_src: '{{ var('load_src') }}'
    load_dt: CAST('{{ var('load_dt') }}' AS DATE)
    effective_from: user_reg_dt
hashed_columns:
    hk_user_id: user_id
    hk_message_id: message_id
    hk_user_message:
      - user_id
      - message_id
    s_user_hashdiff: 
        is_hashdiff: true
        columns: 
          - user_id
          - chat_name
          - user_reg_dt
          - country
          - age
          - effective_from
{% endset %}

{% set metadata_dict = fromyaml(yaml_metadata) %}
{% do log('metadata_dict: ' ~metadata_dict, info=true) %}

{% set source_model = metadata_dict['source_model'] %}
{% set derived_columns = metadata_dict['derived_columns'] %}
{% set hashed_columns = metadata_dict['hashed_columns'] %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=source_model,
                     derived_columns=derived_columns,
                     null_columns=null_columns,
                     hashed_columns=hashed_columns,
                     ranked_columns=ranked_columns) }}



{#
{% set include_source_columns = metadata_dict('include_source_columns') %}
{% set null_columns = metadata_dict('null_columns') %}
{% set ranked_columns = metadata_dict('ranked_columns') %}
#}