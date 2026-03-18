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

# Sales vs Temperature

<BigValue
    data={peak_temp}
    value="temperature_mean"
    title="Temperature At Highest Revenue (°C)"
/>

<PageBreak/>

<BigValue color=#576f8a
    data={peak_temp}
    value="total_revenue"
    title="Revenue (USD) At Temperature"
/>

## All locations

<ScatterPlot
            data={all_locations}
            x="temperature_mean"
            y="total_revenue"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />

## Sales vs Temperature For Each Store Location

<Tabs color=#4971a6>
    <Tab label="Lower Manhattan">

        ## Lower Manhattan (Sales vs Temperature)
        <ScatterPlot
            data={WS_LM}
            x="temperature_mean"
            y="total_revenue"
            series="store_location"
            xAxisTitle= "Temperatur (°C)"
            yAxisTitle= "Revenue (USD)"
        />
    </Tab>
    <Tab label="Astoria">

        ## Astoria (Sales vs Temperature)
        <ScatterPlot
            data={WS_A}
            x="temperature_mean"
            y="total_revenue"
            series="store_location"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />
    </Tab>

    <Tab label="Hell's Kitchen">

        ## Hell's Kitchen (Sales vs Temperature)
        <ScatterPlot
            data={WS_HK}
            x="temperature_mean"
            y="total_revenue"
            series="store_location"
            xAxisTitle="Temperatur (°C)"
            yAxisTitle="Revenue (USD)"
        />

    </Tab>

</Tabs>
