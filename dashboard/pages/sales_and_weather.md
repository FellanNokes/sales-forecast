```sql weather_sales
SELECT * FROM supabase.weather_sales_summary
```

```sql WS_LM
SELECT * FROM supabase.weather_sales_summary
WHERE store_location = 'Lower Manhattan'
```

```sql WS_A
SELECT * FROM supabase.weather_sales_summary
WHERE store_location = 'Astoria'
```

## Lower Manhattan (Sales & Weather Correlation)

<ScatterPlot
    data={WS_LM}
    x="temperature_mean"
    y="total_revenue"
    series="store_location"
    xAxisTitle= "Temperatur (°C)"
    yAxisTitle= "Revenue (USD)"
/>

## Astoria (Sales & Weather Correlation)

<ScatterPlot
    data={WS_A}
    x="temperature_mean"
    y="total_revenue"
    series="store_location"
    xAxisTitle="Temperatur (°C)"
    yAxisTitle="Revenue (USD)"
/>

## All locations

<ScatterPlot
    data={weather_sales}
    x="temperature_mean"
    y="total_revenue"
    series="store_location"
    xAxisTitle="Temperatur (°C)"
    yAxisTitle="Revenue (USD)"
/>


## Weather & temperature sales analysis
```sql revenue_by_weather
SELECT 
    weather_condition,
    temp_category,
    COUNT(*) as transactions,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(AVG(transaction_qty * unit_price), 2) as avg_order_value
FROM supabase.sales_weather_joined
GROUP BY weather_condition, temp_category
ORDER BY revenue DESC
```
```sql revenue_by_temp
SELECT 
    temp_category,
    SUM(transaction_qty * unit_price) as revenue,
    COUNT(*) as transactions
FROM supabase.sales_weather_joined
GROUP BY temp_category
ORDER BY revenue DESC
```
```sql revenue_by_weather_condition
SELECT 
    weather_condition,
    SUM(transaction_qty * unit_price) as revenue,
    COUNT(*) as transactions,
    ROUND(AVG(transaction_qty * unit_price), 2) as avg_order_value
FROM supabase.sales_weather_joined
GROUP BY weather_condition
ORDER BY revenue DESC
```
```sql category_by_weather
SELECT 
    weather_condition,
    product_category,
    SUM(transaction_qty * unit_price) as revenue
FROM supabase.sales_weather_joined
GROUP BY weather_condition, product_category
ORDER BY weather_condition, revenue DESC
```
```sql days_per_temp
SELECT 
    temp_category,
    COUNT(DISTINCT transaction_date) as days,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(SUM(transaction_qty * unit_price) / COUNT(DISTINCT transaction_date), 2) as revenue_per_day
FROM supabase.sales_weather_joined
GROUP BY temp_category
ORDER BY days DESC
```

<Grid cols=2>

<BarChart 
    data={revenue_by_temp}
    x=temp_category
    y=revenue
    title="Revenue by temperature category"
    yAxisTitle="Revenue ($)"
    xAxisTitle="Temperature category"
    colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
/>

<BarChart 
    data={revenue_by_weather_condition}
    x=weather_condition
    y=revenue
    title="Revenue by weather condition"
    yAxisTitle="Revenue ($)"
    xAxisTitle="Weather condition"
    colorPalette={['#7F77DD']}
/>

</Grid>

<BarChart 
    data={revenue_by_weather}
    x=weather_condition
    y=revenue
    y2=transactions
    y2SeriesType=line
    series=temp_category
    title="Revenue and transaction count by weather condition and temperature"
    yAxisTitle="Revenue ($)"
    y2AxisTitle="Transactions"
    type=grouped
    colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
/>

<BarChart 
    data={category_by_weather}
    x=weather_condition
    y=revenue
    series=product_category
    title="Product category sales by weather condition"
    yAxisTitle="Revenue ($)"
    type=stacked
/>

<Grid cols=2>

<BarChart 
    data={days_per_temp}
    x=temp_category
    y=days
    title="Number of days per temperature category"
    yAxisTitle="Days"
    xAxisTitle="Temperature category"
    colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
/>

<BarChart 
    data={days_per_temp}
    x=temp_category
    y=revenue_per_day
    title="Revenue per day by temperature category"
    yAxisTitle="Revenue ($)"
    xAxisTitle="Temperature category"
    colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
/>

</Grid>

<DataTable data={revenue_by_weather} title="Full breakdown"/>