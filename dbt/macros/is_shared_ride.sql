{# Macro to determine if the sr_flag in fhv_taxidata represents a shared ride or not #}
{# 1 = shared ride, null = not a shared ride #}

{% macro is_shared_ride(flag) -%}

case try_to_number({{flag}})
    when 1 then 'Shared'
    else 'Non-Shared'
end

{%- endmacro %}