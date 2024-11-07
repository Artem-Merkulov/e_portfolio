{{ config( materialized='incremental' ) }}

{% set source_model = "v_stg_group" %}

{% set src_pk = "hk_admin" %}
{% set src_fk = ["hk_user_id", "hk_group_id"] %}
{% set src_ldts = "load_dt" %}
{% set src_source = "load_src" %}

{{ automate_dv.link(src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts,
                    src_source=src_source, source_model=source_model) }}