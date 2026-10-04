with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="'" ~ var('anchor_date') ~ "'::date",
        end_date="'" ~ var('date_spine_end') ~ "'::date"
    ) }}
)

select
    to_number(to_char(date_day, 'YYYYMMDD'))   as date_key,
    date_day::date                             as date_day,
    dayofweekiso(date_day)                     as day_of_week_iso,
    dayname(date_day)                          as day_name,
    date_trunc('week', date_day)::date         as week_start,
    weekiso(date_day)                          as iso_week,
    date_trunc('month', date_day)::date        as month_start,
    monthname(date_day)                        as month_name,
    quarter(date_day)                          as quarter,
    year(date_day)                             as year,
    dayofweekiso(date_day) in (6, 7)           as is_weekend,
    datediff('day', '{{ var("anchor_date") }}'::date, date_day) as dataset_day_number
from spine
