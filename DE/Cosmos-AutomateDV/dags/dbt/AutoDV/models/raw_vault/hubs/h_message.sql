{{ config( materialized='incremental' ) }}

{% set source_model = "v_stg_message" %}

{% set src_pk = "hk_message_id" %}
{% set src_nk = "message_id" %}
{% set src_ldts = "load_dt" %}
{% set src_source = "load_src" %}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}