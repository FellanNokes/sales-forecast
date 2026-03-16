# Sales Dashboard

## Revenue by Store
```sql revenue_by_store
SELECT * FROM supabase.analytics_revenue_by_store
```
<LineChart data={revenue_by_store} />

## Top 5 Products (All Time)
```sql top5_products
SELECT * FROM supabase.analytics_top5_products
```
<BarChart data={top5_products} />

## Top 5 Products by Month
```sql top5_products_month
SELECT * FROM supabase.analytics_top5_products_month
```
<BarChart data={top5_products_month} />

## Revenue per Month
```sql revenue_per_month
SELECT * FROM supabase.analytics_revenue_per_month
```
<DataTable data={revenue_per_month} />

## Top Day Revenue per Month
```sql top_day_per_month
SELECT * FROM supabase.analytics_top_day_per_month
```
<DataTable data={top_day_per_month} />

## Least Popular Products
```sql least5_popular
SELECT * FROM supabase.analytics_least5_popular
```
<DataTable data={least5_popular} />

## Weather & Sales Summary
```sql weather_sales_summary
SELECT * FROM supabase.weather_sales_summary
```
<DataTable data={weather_sales_summary} />

## Weather Correlation Results
```sql weather_correlation_results
SELECT * FROM supabase.weather_correlation_results
```
<DataTable data={weather_correlation_results} />
