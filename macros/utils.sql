{% macro parse_dbt_results(results) %}
    -- Create a list of parsed results
    {%- set parsed_results = [] %}
    -- Flatten results and add to list
    {% for run_result in results %}
        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}
        -- Get the underlying dbt graph node that was executed
        {% set node = run_result_dict.get('node') %}
        {%- if run_result_dict.get('adapter_response', {}).get('rows_affected') != None -%}
            {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {%- endif -%}
        {%- if run_result_dict.get('adapter_response', {}).get('rows_affected') == None -%}
            {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected') %}
        {%- endif -%}
        {%- if run_result_dict.get('timing', [])[0].get('started_at') != None -%}
            {% set started_at = run_result_dict.get('timing', [])[0].get('started_at') %}
        {%- endif -%}
        {%- if run_result_dict.get('timing', [])[0].get('started_at') == None -%}
            {% set started_at = run_result_dict.get('timing', [])[0].get('started_at') %}
        {%- endif -%}
        {%- if run_result_dict.get('timing', [])[0].get('completed_at') != None -%}
            {% set completed_at = run_result_dict.get('timing', [])[0].get('completed_at') %}
        {%- endif -%}
        {%- if run_result_dict.get('timing', [])[0].get('completed_at') == None -%}
            {% set completed_at = run_result_dict.get('timing', [])[0].get('completed_at') %}
        {%- endif -%}
        {%- if run_result_dict.get('message') != None -%}
            {% set replaced_message = run_result_dict.get('message').replace("'", '"') %}
        {%- endif -%}
        {%- if run_result_dict.get('message') == None -%}
            {% set replaced_message = run_result_dict.get('message') %}
        {%- endif -%}
        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}
        {% set parsed_result_dict = {
                'result_id': invocation_id ~ '.' ~ node.get('unique_id'),
                'invocation_id': invocation_id,
                'unique_id': node.get('unique_id'),
                'database_name': node.get('database'),
                'schema_name': node.get('schema'),
                'name': node.get('name'),
                'resource_type': node.get('resource_type'),
                'status': run_result_dict.get('status'),
                'message': replaced_message,
                'started_at': started_at,
                'completed_at': completed_at,
                'execution_time': run_result_dict.get('execution_time'),
                'rows_affected': rows_affected
                }%}
        {% do parsed_results.append(parsed_result_dict) %}
    {% endfor %}
    {{ return(parsed_results) }}
{% endmacro %}

{% macro log_dbt_results(results) %}
    {%- if execute -%}
        {%- set parsed_results = parse_dbt_results(results) -%}
        {%- if parsed_results | length  > 0 -%}
            {% set insert_dbt_results_query -%}
                insert into [DataWarehouse].[meta].[dbt_results]
                    (
                        result_id,
                        invocation_id,
                        unique_id,
                        database_name,
                        schema_name,
                        name,
                        resource_type,
                        status,
                        message,
                        started_at,
                        completed_at,
                        execution_time,
                        rows_affected
                ) values
                    {%- for parsed_result_dict in parsed_results -%}
                        (
                            '{{ parsed_result_dict.get('result_id') }}',
                            '{{ parsed_result_dict.get('invocation_id') }}',
                            '{{ parsed_result_dict.get('unique_id') }}',
                            '{{ parsed_result_dict.get('database_name') }}',
                            '{{ parsed_result_dict.get('schema_name') }}',
                            '{{ parsed_result_dict.get('name') }}',
                            '{{ parsed_result_dict.get('resource_type') }}',
                            '{{ parsed_result_dict.get('status') }}',
                            '{{ parsed_result_dict.get('message') }}',
                            '{{ parsed_result_dict.get('started_at') }}',
                            '{{ parsed_result_dict.get('completed_at') }}',
                            '{{ parsed_result_dict.get('execution_time') }}',
                            '{{ parsed_result_dict.get('rows_affected') }}'
                        ) {{- "," if not loop.last else "" -}}
                    {%- endfor -%}
            {%- endset -%}
            {%- do run_query(insert_dbt_results_query) -%}
        {%- endif -%}
    {%- endif -%}
    -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
    {{ return ('') }}
{% endmacro %}
