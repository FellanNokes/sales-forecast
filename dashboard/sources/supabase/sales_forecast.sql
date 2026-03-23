SELECT
    id,
    transaction_date,
    transaction_time :: text AS transaction_time,
    transaction_qty,
    store_id,
    store_location,
    product_id,
    unit_price,
    product_category,
    product_type,
    product_detail,
    is_synthetic,
    generated_at
FROM
    sales_forecast