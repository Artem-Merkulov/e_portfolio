{{ config( materialized='view' ) }}

{% set yaml_metadata %}
source_model: raw_stg
derived_columns: 
    load_src: CAST('{{ var('load_src') }}' AS TEXT)
    load_dt: CAST('{{ var('load_dt') }}' AS DATE)
    effective_from: group_reg_dt
hashed_columns:
    hk_user_id: user_id
    hk_group_id: group_id
    hk_admin:
      - user_id
      - group_id
    s_group_hashdiff: 
        is_hashdiff: true
        columns: 
          - group_id
          - admin_id
          - group_name
          - group_reg_dt
          - is_private
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