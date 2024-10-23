{{ config( materialized='incremental' ) }}

{% set source_model = "v_stg_user" %}

{% set src_pk = "hk_user_id" %}
{% set src_hashdiff = {"source_column": "s_user_hashdiff", "alias": "s_user_hashdiff"} %}
{% set src_payload = ["chat_name", "user_reg_dt"] %}
{% set src_eff = "effective_from" %}
{% set src_ldts = "load_dt" %}
{% set src_source = "load_src" %}

{{ automate_dv.sat(src_pk=src_pk, 
                   src_hashdiff=src_hashdiff, 
                   src_payload=src_payload,
                   src_eff=src_eff, 
                   src_ldts=src_ldts, 
                   src_source=src_source,
                   source_model=source_model) }}