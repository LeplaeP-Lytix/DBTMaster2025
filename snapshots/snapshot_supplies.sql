{% snapshot snapshot_supplies %}

{{
    config(
        tags=["snapshot"] ,
        strategy='check',
        unique_key='supply_uuid',
        check_cols=['supply_uuid', 'supply_id','product_id','supply_name','supply_cost','is_perishable_supply'],
        invalidate_hard_deletes=True,
    )   
}}

select * from {{ ref('stg_supplies') }}

{% endsnapshot %}