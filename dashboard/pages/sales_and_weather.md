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
