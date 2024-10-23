{{ config( materialized='incremental' ) }}

{% set source_model = "v_stg_group" %}

{% set src_pk = "hk_group_id" %}
{% set src_nk = "group_id" %}
{% set src_ldts = "load_dt" %}
{% set src_source = "load_src" %}

{{ automate_dv.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                   src_source=src_source, source_model=source_model) }}