## Top 5 Products by Month

```sql top5_products_month
SELECT * FROM supabase.analytics_top5_products_month
```

### Choose Month Here:

<Dropdown
  data={top5_products_month}
  name=month
  value=month
  label=month
/>

## Lower Manhattan

```sql top_LM
SELECT *
FROM supabase.analytics_top5_products_month
WHERE store_id = 5
AND month = '${inputs.month.value}'
```

<BarChart data={top_LM} x=product_type y=revenue_per_day sort=-revenue_per_day />

## Astoria

```sql top_A
SELECT *
FROM supabase.analytics_top5_products_month
WHERE store_id = 3
AND month = '${inputs.month.value}'
```

<BarChart data={top_A} x=product_type y=revenue_per_day sort=-revenue_per_day />

## Hell's Kitchen

```sql top_HK
SELECT *
FROM supabase.analytics_top5_products_month
WHERE store_id = 8
AND month = '${inputs.month.value}'
```

<BarChart data={top_HK} x=product_type y=revenue_per_day sort=-revenue_per_day />
