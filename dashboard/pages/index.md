```sql all_locations
SELECT
  temperature_mean,
  SUM(total_revenue) AS total_revenue
FROM supabase.weather_sales_summary
GROUP BY temperature_mean
ORDER BY temperature_mean
```

```sql WS_LM
SELECT * FROM supabase.weather_sales_summary
WHERE store_location = 'Lower Manhattan'
```

```sql WS_A
SELECT * FROM supabase.weather_sales_summary
WHERE store_location = 'Astoria'
```

```sql WS_HK
SELECT * FROM supabase.weather_sales_summary
WHERE store_location NOT IN ('Astoria', 'Lower Manhattan')
```

```sql peak_temp
SELECT
  temperature_mean,
  SUM(total_revenue) AS total_revenue
FROM supabase.weather_sales_summary
GROUP BY temperature_mean
ORDER BY total_revenue DESC
LIMIT 1
```

# Sales vs Mean Temperature

<BigValue
    data={peak_temp}
    value="temperature_mean"
    title="Mean Temperature At Highest Revenue (°C)"
/>

<BigValue color=#576f8a
    data={peak_temp}
    value="total_revenue"
    title="Revenue (USD) At Mean Temperature"
/>

<Tabs color=#4971a6>
    <Tab label = "All Locations">
        <ScatterPlot
            data={all_locations}
            title = "All locations (Sales vs Mean Temperature)"
            x="temperature_mean"
            y="total_revenue"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <ScatterPlot
            title = "Lower Manhattan (Sales vs Mean Temperature)"
            data={WS_LM}
            x="temperature_mean"
            y="total_revenue"
            series="store_location"
            xAxisTitle= "Temperatur (°C)"
            yAxisTitle= "Revenue (USD)"
        />
    </Tab>
    <Tab label="Astoria">
        <ScatterPlot
            title = " Astoria (Sales vs Mean Temperature)"
            data={WS_A}
            x="temperature_mean"
            y="total_revenue"
            series="store_location"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />
    </Tab>

    <Tab label="Hell's Kitchen">
        <ScatterPlot
            title = "Hell's Kitchen (Sales vs Mean Temperature)"
            data={WS_HK}
            x="temperature_mean"
            y="total_revenue"
            series="store_location"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />

    </Tab>

</Tabs>

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

```sql days_per_temp_LM
SELECT
    temp_category,
    COUNT(DISTINCT transaction_date) as days,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(SUM(transaction_qty * unit_price) / COUNT(DISTINCT transaction_date), 2) as revenue_per_day
FROM supabase.sales_weather_joined
WHERE store_location = 'Lower Manhattan'
GROUP BY temp_category
ORDER BY days DESC
```

```sql days_per_temp_A
SELECT
    temp_category,
    COUNT(DISTINCT transaction_date) as days,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(SUM(transaction_qty * unit_price) / COUNT(DISTINCT transaction_date), 2) as revenue_per_day
FROM supabase.sales_weather_joined
WHERE store_location = 'Astoria'
GROUP BY temp_category
ORDER BY days DESC
```

```sql days_per_temp_HK
SELECT
    temp_category,
    COUNT(DISTINCT transaction_date) as days,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(SUM(transaction_qty * unit_price) / COUNT(DISTINCT transaction_date), 2) as revenue_per_day
FROM supabase.sales_weather_joined
WHERE store_location NOT IN ('Lower Manhattan', 'Astoria')
GROUP BY temp_category
ORDER BY days DESC
```

# Avrage Temperature Per Day By Temperature Categories

<Tabs color=#4971a6>
    <Tab label="All stores">
        <BarChart
        data={days_per_temp}
        x=temp_category
        y=revenue_per_day
        title="Avg revenue per day by temperature category — all stores"
        yAxisTitle="Revenue ($)"
        xAxisTitle="Temperature category"
        colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />              
    </Tab>
    <Tab label="Lower Manhattan">
        <BarChart
            data={days_per_temp_LM}
            x=temp_category
            y=revenue_per_day
            title="Avg revenue per day by temperature category — Lower Manhattan"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Astoria">
        <BarChart
            data={days_per_temp_A}
            x=temp_category
            y=revenue_per_day
            title="Avg revenue per day by temperature category — Astoria"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <BarChart
            data={days_per_temp_HK}
            x=temp_category
            y=revenue_per_day
            title="Avg revenue per day by temperature category — Hell's Kitchen"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
            />
    </Tab>
</Tabs>

# Avrage Revenue By Weather Condition

```sql days_per_weather
SELECT
    weather_condition,
    COUNT(DISTINCT transaction_date) as days,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(SUM(transaction_qty * unit_price) / COUNT(DISTINCT transaction_date), 2) as revenue_per_day
FROM supabase.sales_weather_joined
GROUP BY weather_condition
ORDER BY days DESC
```

```sql days_per_weather_LM
SELECT
    weather_condition,
    COUNT(DISTINCT transaction_date) as days,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(SUM(transaction_qty * unit_price) / COUNT(DISTINCT transaction_date), 2) as revenue_per_day
FROM supabase.sales_weather_joined
WHERE store_location = 'Lower Manhattan'
GROUP BY weather_condition
ORDER BY days DESC
```

```sql days_per_weather_A
SELECT
    weather_condition,
    COUNT(DISTINCT transaction_date) as days,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(SUM(transaction_qty * unit_price) / COUNT(DISTINCT transaction_date), 2) as revenue_per_day
FROM supabase.sales_weather_joined
WHERE store_location = 'Astoria'
GROUP BY weather_condition
ORDER BY days DESC
```

```sql days_per_weather_HK
SELECT
    weather_condition,
    COUNT(DISTINCT transaction_date) as days,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(SUM(transaction_qty * unit_price) / COUNT(DISTINCT transaction_date), 2) as revenue_per_day
FROM supabase.sales_weather_joined
WHERE store_location NOT IN ('Lower Manhattan', 'Astoria')
GROUP BY weather_condition
ORDER BY days DESC
```

<Tabs color=#4971a6>  
    <Tab label="All stores">
        <BarChart
            data={days_per_weather}
            x=weather_condition
            y=revenue_per_day
            title="Avg revenue per day by weather condition — all stores"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <BarChart
            data={days_per_weather_LM}
            x=weather_condition
            y=revenue_per_day
            title="Avg revenue per day by weather condition — Lower Manhattan"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Astoria">
        <BarChart
            data={days_per_weather_A}
            x=weather_condition
            y=revenue_per_day
            title="Avg revenue per day by weather condition — Astoria"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <BarChart
            data={days_per_weather_HK}
            x=weather_condition
            y=revenue_per_day
            title="Avg revenue per day by weather condition — Hell's Kitchen"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
</Tabs>
