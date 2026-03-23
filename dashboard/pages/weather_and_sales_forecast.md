```sql weather_forecast
SELECT * FROM supabase.weather_forecast
ORDER BY date
```

```sql sales_forecast
SELECT
    *
FROM
    supabase.sales_forecast
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

## Overview of Sales Forecast

<DataTable data={sales_forecast}/>
