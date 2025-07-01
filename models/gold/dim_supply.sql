with

supplies as (

    select * from {{ ref('snapshot_supplies') }}

)

select * from supplies
