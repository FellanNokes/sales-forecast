---
title: Sales Prediction
---

```sql predictions
SELECT
    prediction_date,
    store_location,
    ROUND(predicted_revenue, 2) as predicted_revenue,
    temperature_mean,
    weather_condition,
    temp_category,
    CASE day_of_week
        WHEN 0 THEN 'Monday'
        WHEN 1 THEN 'Tuesday'
        WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday'
        WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday'
        WHEN 6 THEN 'Sunday'
    END as weekday,
    day_of_week
FROM supabase.sales_prediction
ORDER BY prediction_date, store_location
```

```sql predictions_lm
SELECT * FROM ${predictions}
WHERE store_location = 'Lower Manhattan'
```

```sql predictions_hk
SELECT * FROM ${predictions}
WHERE store_location = 'Hell''s Kitchen'
```

```sql predictions_a
SELECT * FROM ${predictions}
WHERE store_location = 'Astoria'
```

```sql by_weekday_and_weather
SELECT
    weekday,
    day_of_week,
    weather_condition,
    ROUND(AVG(predicted_revenue), 2) as avg_predicted_revenue
FROM ${predictions}
GROUP BY weekday, day_of_week, weather_condition
ORDER BY day_of_week, weather_condition
```

```sql by_weekday
SELECT
    weekday,
    day_of_week,
    ROUND(AVG(predicted_revenue), 2) as avg_predicted_revenue
FROM ${predictions}
GROUP BY weekday, day_of_week
ORDER BY day_of_week
```

```sql by_weather
SELECT
    weather_condition,
    ROUND(AVG(predicted_revenue), 2) as avg_predicted_revenue
FROM ${predictions}
GROUP BY weather_condition
ORDER BY avg_predicted_revenue DESC
```

# 14-Day Sales Prediction

> **Note:** Model trained on synthetic data (R² = 0.15).
> Predictions will improve significantly with real sales data.
> Model type: Linear Regression. Features: temperature, rain, weather condition, temperature category, day of week, month.

---

## Predicted Revenue — Next 14 Days

<Tabs>
    <Tab label="All Stores">
        <LineChart
            data={predictions}
            x=prediction_date
            y=predicted_revenue
            series=store_location
            title="Predicted Revenue — All Stores"
            yAxisTitle="Predicted Revenue ($)"
            xAxisTitle="Date"
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <LineChart
            data={predictions_lm}
            x=prediction_date
            y=predicted_revenue
            title="Predicted Revenue — Lower Manhattan"
            yAxisTitle="Predicted Revenue ($)"
            xAxisTitle="Date"
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <LineChart
            data={predictions_hk}
            x=prediction_date
            y=predicted_revenue
            title="Predicted Revenue — Hell's Kitchen"
            yAxisTitle="Predicted Revenue ($)"
            xAxisTitle="Date"
        />
    </Tab>
    <Tab label="Astoria">
        <LineChart
            data={predictions_a}
            x=prediction_date
            y=predicted_revenue
            title="Predicted Revenue — Astoria"
            yAxisTitle="Predicted Revenue ($)"
            xAxisTitle="Date"
        />
    </Tab>
</Tabs>

---

## Average Predicted Revenue by Day of Week

<BarChart
data={by_weekday}
x=weekday
y=avg_predicted_revenue
title="Avg Predicted Revenue by Day of Week — All Stores"
yAxisTitle="Predicted Revenue ($)"
xAxisTitle="Day of Week"
colorPalette={['#378ADD']}
sort=false
/>

---

## Day of Week × Weather Condition

This chart shows how the combination of weekday and weather
affects predicted revenue — e.g. a rainy Monday vs a clear Friday.

<BarChart
    data={by_weekday_and_weather}
    x=weekday
    y=avg_predicted_revenue
    series=weather_condition
    title="Predicted Revenue by Day of Week and Weather Condition"
    yAxisTitle="Predicted Revenue ($)"
    xAxisTitle="Day of Week"
    type=grouped
    sort=false
/>

---

## Average Predicted Revenue by Weather Condition

<BarChart
data={by_weather}
x=weather_condition
y=avg_predicted_revenue
title="Avg Predicted Revenue by Weather Condition"
yAxisTitle="Predicted Revenue ($)"
xAxisTitle="Weather Condition"
colorPalette={['#7F77DD']}
sort=false
/>

---

## Full Prediction Table

<DataTable
    data={predictions}
    rows=14
/>

```sql weather_forecast
SELECT * FROM supabase.weather_forecast
ORDER BY date
```

```sql sales_forecast
SELECT
    *
FROM
    supabase.daily_salesdata
```

# Forecast Weather And Sales

---

## Overview of Weather Forecast

<DataTable data={weather_forecast}>
<Column id=date/>
<Column id=store_location/>
<Column id=temperature_mean/>
<Column id=temperature_max/>
<Column id=temperature_min/>
<Column id=rain_sum/>
<Column id=snowfall_sum/>
<Column id=wind_speed_10m_max/>
<Column id=fetched_at title="Updated At"/>
</DataTable>

---

## Overview Over Daily Sales Data

<DataTable data={sales_forecast}/>
