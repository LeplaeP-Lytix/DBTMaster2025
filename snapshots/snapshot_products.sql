{% snapshot snapshot_products %}

{{
    config(
        tags=["snapshot"] ,
        strategy='check',
        unique_key='product_id',
        check_cols=['product_id', 'product_name','product_type','product_description','product_price','is_food_item','is_drink_item'],
        invalidate_hard_deletes=True,
    )   
}}

select * from {{ ref('stg_products') }}

{% endsnapshot %}