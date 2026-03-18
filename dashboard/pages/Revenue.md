```sql revenue_per_month_store
SELECT
  month,
  store_location,
  SUM(revenue) as revenue,
  CASE month
    WHEN 'January'   THEN 1
    WHEN 'February'  THEN 2
    WHEN 'March'     THEN 3
    WHEN 'April'     THEN 4
    WHEN 'May'       THEN 5
    WHEN 'June'      THEN 6
  END AS month_order
FROM supabase.analytics_revenue_per_month
GROUP BY month, store_location
ORDER BY month_order
```

```sql revenue_per_month
SELECT
  month,
  SUM(revenue) as revenue,
  CASE month
    WHEN 'January'   THEN 1
    WHEN 'February'  THEN 2
    WHEN 'March'     THEN 3
    WHEN 'April'     THEN 4
    WHEN 'May'       THEN 5
    WHEN 'June'      THEN 6
  END AS month_order
FROM supabase.analytics_revenue_per_month
GROUP BY month
ORDER BY month_order
```

```sql revenue
SELECT SUM(revenue) AS revenue
FROM supabase.analytics_revenue_per_month
```

## Total Revenue Jan-June

<BigValue 
    data={revenue}
    value=revenue
    fmt=usd
/>

## Total Sales For Sales Period (Jan-June)

<BarChart
        data={revenue_per_month}
        x=month
        y=revenue
        yAxisTitle="Revenue (USD)"
        sort=false
    />

## Sales Per Month (Store Location)

<Heatmap
    borders="red"
    data={revenue_per_month_store}
    x=store_location
    y=month
    value=revenue
    valueFmt=usd
/>
