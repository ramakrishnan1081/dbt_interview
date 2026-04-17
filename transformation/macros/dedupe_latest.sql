{% macro dedupe_latest(model, partition_by, order_by='systemmodstamp') %}

select *
from (
    select *,
           row_number() over (
               partition by {{ partition_by }}
               order by {{ order_by }} desc
           ) as rn
    from {{ model }}
)
where rn = 1

{% endmacro %}