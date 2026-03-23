```sql weather_sales
SELECT * FROM supabase.weather_sales_summary
```

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

# Weather vs Temperature Analysis

---

## Sales vs Mean Temperature

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

<Tabs fullWidth=true color=#4971a6>
    <Tab label="All Locations">
        <ScatterPlot
            data={all_locations}
            title="All locations (Sales vs Mean Temperature)"
            x="temperature_mean"
            y="total_revenue"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <ScatterPlot
            title="Lower Manhattan (Sales vs Mean Temperature)"
            data={WS_LM}
            x="temperature_mean"
            y="total_revenue"
            series="store_location"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />
    </Tab>
    <Tab label="Astoria">
        <ScatterPlot
            title="Astoria (Sales vs Mean Temperature)"
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
            title="Hell's Kitchen (Sales vs Mean Temperature)"
            data={WS_HK}
            x="temperature_mean"
            y="total_revenue"
            series="store_location"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />
    </Tab>
</Tabs>

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

```sql revenue_by_weather_LM
SELECT
    weather_condition,
    temp_category,
    COUNT(*) as transactions,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(AVG(transaction_qty * unit_price), 2) as avg_order_value
FROM supabase.sales_weather_joined
WHERE store_location = 'Lower Manhattan'
GROUP BY weather_condition, temp_category
ORDER BY revenue DESC
```

```sql revenue_by_weather_A
SELECT
    weather_condition,
    temp_category,
    COUNT(*) as transactions,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(AVG(transaction_qty * unit_price), 2) as avg_order_value
FROM supabase.sales_weather_joined
WHERE store_location = 'Astoria'
GROUP BY weather_condition, temp_category
ORDER BY revenue DESC
```

```sql revenue_by_weather_HK
SELECT
    weather_condition,
    temp_category,
    COUNT(*) as transactions,
    SUM(transaction_qty * unit_price) as revenue,
    ROUND(AVG(transaction_qty * unit_price), 2) as avg_order_value
FROM supabase.sales_weather_joined
WHERE store_location NOT IN ('Lower Manhattan', 'Astoria')
GROUP BY weather_condition, temp_category
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

```sql category_by_weather_LM
SELECT
    weather_condition,
    product_category,
    SUM(transaction_qty * unit_price) as revenue
FROM supabase.sales_weather_joined
WHERE store_location = 'Lower Manhattan'
GROUP BY weather_condition, product_category
ORDER BY weather_condition, revenue DESC
```

```sql category_by_weather_A
SELECT
    weather_condition,
    product_category,
    SUM(transaction_qty * unit_price) as revenue
FROM supabase.sales_weather_joined
WHERE store_location = 'Astoria'
GROUP BY weather_condition, product_category
ORDER BY weather_condition, revenue DESC
```

```sql category_by_weather_HK
SELECT
    weather_condition,
    product_category,
    SUM(transaction_qty * unit_price) as revenue
FROM supabase.sales_weather_joined
WHERE store_location NOT IN ('Lower Manhattan', 'Astoria')
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

## Avrage Temperature Per Day By Temperature Categories

<Tabs fullWidth=true color=#4971a6>
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

---

## Total Revenue By Temperature Category

<Tabs fullWidth=true color=#4971a6>
    <Tab label="All stores">
        <BarChart
            data={days_per_temp}
            x=temp_category
            y=revenue
            title="Total revenue by temperature category — all stores"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <BarChart
            data={days_per_temp_LM}
            x=temp_category
            y=revenue
            title="Total revenue by temperature category — Lower Manhattan"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Astoria">
        <BarChart
            data={days_per_temp_A}
            x=temp_category
            y=revenue
            title="Total revenue by temperature category — Astoria"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <BarChart
            data={days_per_temp_HK}
            x=temp_category
            y=revenue
            title="Total revenue by temperature category — Hell's Kitchen"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
</Tabs>

---

## Number Of Days Per Temperature Category

<Tabs fullWidth=true color=#4971a6>
    <Tab label="All stores">
        <BarChart
            data={days_per_temp}
            x=temp_category
            y=days
            title="Number of days per temperature category — all stores"
            yAxisTitle="Days"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <BarChart
            data={days_per_temp_LM}
            x=temp_category
            y=days
            title="Number of days per temperature category — Lower Manhattan"
            yAxisTitle="Days"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Astoria">
        <BarChart
            data={days_per_temp_A}
            x=temp_category
            y=days
            title="Number of days per temperature category — Astoria"
            yAxisTitle="Days"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <BarChart
            data={days_per_temp_HK}
            x=temp_category
            y=days
            title="Number of days per temperature category — Hell's Kitchen"
            yAxisTitle="Days"
            xAxisTitle="Temperature category"
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
</Tabs>

---

## Avrage Revenue Per Day By Weather Condition

<Tabs fullWidth=true color=#4971a6>
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

---

## Total Revenue By Weather Condition

<Tabs fullWidth=true color=#4971a6>
    <Tab label="All stores">
        <BarChart
            data={days_per_weather}
            x=weather_condition
            y=revenue
            title="Total revenue by weather condition — all stores"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <BarChart
            data={days_per_weather_LM}
            x=weather_condition
            y=revenue
            title="Total revenue by weather condition — Lower Manhattan"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Astoria">
        <BarChart
            data={days_per_weather_A}
            x=weather_condition
            y=revenue
            title="Total revenue by weather condition — Astoria"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <BarChart
            data={days_per_weather_HK}
            x=weather_condition
            y=revenue
            title="Total revenue by weather condition — Hell's Kitchen"
            yAxisTitle="Revenue ($)"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
</Tabs>

---

## Number Of Days Per Weather Condition

<Tabs fullWidth=true color=#4971a6>
    <Tab label="All stores">
        <BarChart
            data={days_per_weather}
            x=weather_condition
            y=days
            title="Number of days per weather condition — all stores"
            yAxisTitle="Days"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <BarChart
            data={days_per_weather_LM}
            x=weather_condition
            y=days
            title="Number of days per weather condition — Lower Manhattan"
            yAxisTitle="Days"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Astoria">
        <BarChart
            data={days_per_weather_A}
            x=weather_condition
            y=days
            title="Number of days per weather condition — Astoria"
            yAxisTitle="Days"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <BarChart
            data={days_per_weather_HK}
            x=weather_condition
            y=days
            title="Number of days per weather condition — Hell's Kitchen"
            yAxisTitle="Days"
            xAxisTitle="Weather condition"
            colorPalette={['#7F77DD']}
        />
    </Tab>
</Tabs>

---

## Weather & temperature combined

<Tabs fullWidth=true color=#4971a6>
    <Tab label="All stores">
        <BarChart
            data={revenue_by_weather}
            x=weather_condition
            y=revenue
            y2=transactions
            y2SeriesType=line
            series=temp_category
            title="Revenue and transaction count by weather condition and temperature — all stores"
            yAxisTitle="Revenue ($)"
            y2AxisTitle="Transactions"
            type=grouped
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <BarChart
            data={revenue_by_weather_LM}
            x=weather_condition
            y=revenue
            y2=transactions
            y2SeriesType=line
            series=temp_category
            title="Revenue and transaction count by weather condition and temperature — Lower Manhattan"
            yAxisTitle="Revenue ($)"
            y2AxisTitle="Transactions"
            type=grouped
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Astoria">
        <BarChart
            data={revenue_by_weather_A}
            x=weather_condition
            y=revenue
            y2=transactions
            y2SeriesType=line
            series=temp_category
            title="Revenue and transaction count by weather condition and temperature — Astoria"
            yAxisTitle="Revenue ($)"
            y2AxisTitle="Transactions"
            type=grouped
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <BarChart
            data={revenue_by_weather_HK}
            x=weather_condition
            y=revenue
            y2=transactions
            y2SeriesType=line
            series=temp_category
            title="Revenue and transaction count by weather condition and temperature — Hell's Kitchen"
            yAxisTitle="Revenue ($)"
            y2AxisTitle="Transactions"
            type=grouped
            colorPalette={['#378ADD', '#1D9E75', '#EF9F27', '#D85A30']}
        />
    </Tab>
</Tabs>

---

## Product Category Sales Based On Weather

<Tabs fullWidth=true color=#4971a6>
    <Tab label="All stores">
        <BarChart
            data={category_by_weather}
            x=weather_condition
            y=revenue
            series=product_category
            title="Product category sales by weather condition — all stores"
            yAxisTitle="Revenue ($)"
            type=stacked
        />
    </Tab>
    <Tab label="Lower Manhattan">
        <BarChart
            data={category_by_weather_LM}
            x=weather_condition
            y=revenue
            series=product_category
            title="Product category sales by weather condition — Lower Manhattan"
            yAxisTitle="Revenue ($)"
            type=stacked
        />
    </Tab>
    <Tab label="Astoria">
        <BarChart
            data={category_by_weather_A}
            x=weather_condition
            y=revenue
            series=product_category
            title="Product category sales by weather condition — Astoria"
            yAxisTitle="Revenue ($)"
            type=stacked
        />
    </Tab>
    <Tab label="Hell's Kitchen">
        <BarChart
            data={category_by_weather_HK}
            x=weather_condition
            y=revenue
            series=product_category
            title="Product category sales by weather condition — Hell's Kitchen"
            yAxisTitle="Revenue ($)"
            type=stacked
        />
    </Tab>
</Tabs>

---

<DataTable data={revenue_by_weather} title="Full breakdown — all stores"/>
