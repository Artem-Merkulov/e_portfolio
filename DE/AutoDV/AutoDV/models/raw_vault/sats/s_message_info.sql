{{ config( materialized='incremental' ) }}

{% set source_model = "v_stg_message" %}

{% set src_pk = "hk_message_id" %}
{% set src_hashdiff = {"source_column": "s_message_hashdiff", "alias": "s_message_hashdiff"} %}
{% set src_payload = ["message_ts", "message_from", "message_to", "message", "message_group"] %}
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