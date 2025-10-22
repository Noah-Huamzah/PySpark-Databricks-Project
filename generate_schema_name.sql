{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}

  {%- if custom_schema_name is none -%}
    {{ default_schema }}
  {%- else -%}
    {# Example: Prepend environment to custom schema name #}
    {%- set environment = env_var('DBT_ENVIRONMENT', 'development') -%}
    {%- if environment == 'production' -%}
      {{ custom_schema_name | trim }} {# In production, use only the custom schema name #}
    {%- else -%}
      {{ custom_schema_name | trim }} {# In other environments, combine default and custom #}
    {%- endif -%}
  {%- endif -%}
{%- endmacro %}