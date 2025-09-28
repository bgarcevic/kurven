with rema_products as (
    select
        store_name,
        product_id,
        product_name,
        brand,
        underline,
        image_url,
        department_name,
        category_name,
        current_price,
        normal_price,
        price_per_kilogram,
        price_per_unit,
        price_changes_on,
        price_changes_type,
        is_on_discount,
        is_advertised,
        split_part(underline, '/', 1) as amount_unit,
        split_part(amount_unit, ' ', 1) as amount,
        lower(replace(split_part(amount_unit, ' ', 2), '.', '')) as unit,
        round(normal_price - current_price, 2) as discount_amount,
        round(discount_amount / nullif(normal_price, 0), 4)
            as discount_percentage,
        _dlt_load_id
    from {{ ref('stg_rema__products') }}
),

nemlig_products as (
    select
        store_name,
        product_id,
        product_name,
        brand,
        description,
        image_url,
        department_name,
        category_name,
        current_price,
        normal_price,
        price_per_kilogram,
        price_per_unit,
        price_changes_on,
        price_changes_type,
        is_on_discount,
        null as is_advertised,
        split_part(description, '/', 1) as amount_unit,
        regexp_extract(
            description,
            '^([0-9]+(?:[,.][0-9]+)?(?:\s*x\s*[0-9]+(?:[,.][0-9]+)?)?)'
        ) as amount,
        split_part(
            regexp_extract(
                description,
                '^[0-9]+(?:[,.][0-9]+)?(?:\s*x\s*[0-9]+(?:[,.][0-9]+)?)?\s+([a-zA-Z]+)'
            ),
            ' ',
            2
        ) as unit,
        round(normal_price - current_price, 2) as discount_amount,
        round(discount_amount / nullif(normal_price, 0), 4)
            as discount_percentage,
        _dlt_load_id
    from {{ ref('stg_nemlig__products') }}
),

final as (
    select * exclude (amount_unit)
    from rema_products
    union all
    select * exclude (amount_unit)
    from nemlig_products
)

select * from final
