with

source as (

    select * from {{ source('ecom', 'raw_products') }}

),

renamed as (

    select

        ----------  ids
        sku as product_id,

        ---------- text
        [name] as product_name,
        [type] as product_type,
        [description] as product_description,


        ---------- numerics
        {{ cents_to_dollars('price') }} as product_price,

        ---------- booleans
        case 
        when [type] = 'jaffle' then 1
        else 0
        END as is_food_item,
        
        case 
        when [type] = 'beverage' then 1
        else 0
        END as is_drink_item

    from source

)

select * from renamed
