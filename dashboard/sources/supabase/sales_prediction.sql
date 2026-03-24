SELECT
    id,
    prediction_date,
    store_location,
    predicted_revenue,
    temperature_mean,
    weather_condition,
    temp_category,
    day_of_week,
    model_version,
    created_at
FROM sales_prediction
ORDER BY prediction_date, store_location