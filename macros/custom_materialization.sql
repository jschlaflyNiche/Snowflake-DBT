{% materialization table_custom, default %}
  {%- set identifier = model['alias'] -%}
  {%- set tmp_identifier = model['name'] + '_stage' -%}

  {%- set proc_call = "CALL FRAMEWORK_STAGE_DATA_MERGE ('" + database + "', '" + schema + "', '" + identifier + "');" -%}

  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- set intermediate_relation = api.Relation.create(identifier=tmp_identifier,
                                                      schema=schema,
                                                      database=database,
                                                      type='table') -%}

  /*
      See ../view/view.sql for more information about this relation.
  */

  -- drop the temp relations if they exists for some reason
  {{ adapter.drop_relation(intermediate_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, intermediate_relation, sql) }}
  {%- endcall %}


  {% do run_query(proc_call) %}

  -- cleanup

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
--   {{ adapter.commit() }}

  -- finally, drop the existing/backup relation after the commit

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}