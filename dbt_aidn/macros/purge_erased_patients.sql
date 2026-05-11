{% macro purge_erased_patients() %}
  {#
    Hard-deletes all raw.* rows for patients with a pending erasure request.

    Reads main.erasure_requests where erased_at IS NULL, issues one DELETE per
    target table, logs rows_deleted per table, then stamps erased_at.
    Re-running is a no-op: the erased_at IS NULL guard filters completed requests.

    Documented seam: this macro issues DML against the raw.* schema that dlt
    normally owns. Erasure requests are out-of-band controller operations; dlt's
    idempotency is unaffected because pg_replication only forwards new WAL events
    and will not re-emit rows whose source-side delete already propagated.
  #}

  {%- set pending_sql -%}
    select patient_id from main.erasure_requests where erased_at is null
  {%- endset -%}

  {%- set pending_result = run_query(pending_sql) -%}
  {%- set patient_ids = pending_result.columns[0].values() -%}

  {% if patient_ids | length == 0 %}
    {{ log("erasure_noop reason=no_pending_requests", info=true) }}
    {{ return(none) }}
  {% endif %}

  {%- set id_clause = "'" ~ (patient_ids | join("','")) ~ "'" -%}

  {% set targets = ["raw.patient_consents", "raw.appointments", "raw.patients"] %}

  {% for table in targets %}

    {%- set count_sql -%}
      select count(*) from {{ table }} where patient_id in ({{ id_clause }})
    {%- endset -%}
    {%- set n = run_query(count_sql).columns[0].values()[0] -%}

    {%- set delete_sql -%}
      delete from {{ table }} where patient_id in ({{ id_clause }})
    {%- endset -%}
    {% do run_query(delete_sql) %}

    {{ log("rows_deleted table=" ~ table ~ " count=" ~ n ~ " patient_ids=" ~ (patient_ids | length), info=true) }}

  {% endfor %}

  {%- set stamp_sql -%}
    update main.erasure_requests
    set erased_at = current_timestamp
    where patient_id in ({{ id_clause }}) and erased_at is null
  {%- endset -%}
  {% do run_query(stamp_sql) %}

  {{ log("erasure_complete patient_ids=" ~ (patient_ids | length), info=true) }}

{% endmacro %}
