```sql top5_products_month
SELECT DISTINCT month FROM supabase.analytics_top5_products_month ORDER BY month
```

```sql top_LM
SELECT *
FROM supabase.analytics_top5_products_month
WHERE store_id = 5
AND month = '${inputs.month.value}'
```

```sql top_A
SELECT *
FROM supabase.analytics_top5_products_month
WHERE store_id = 3
AND month = '${inputs.month.value}'
```

```sql top_HK
SELECT *
FROM supabase.analytics_top5_products_month
WHERE store_id = 8
AND month = '${inputs.month.value}'
```

```sql top5_LM
SELECT * FROM supabase.analytics_top5_products
WHERE store_location = 'Lower Manhattan'
```

```sql top5_A
SELECT * FROM supabase.analytics_top5_products
WHERE store_location = 'Astoria'
```

```sql top5_HK
SELECT * FROM supabase.analytics_top5_products
WHERE store_location NOT IN ('Lower Manhattan', 'Astoria')
```

```sql least5_LM
SELECT * FROM supabase.analytics_least5_popular
WHERE store_location = 'Lower Manhattan'
```

```sql least5_A
SELECT * FROM supabase.analytics_least5_popular
WHERE store_location = 'Astoria'
```

```sql least5_HK
SELECT * FROM supabase.analytics_least5_popular
WHERE store_location NOT IN ('Lower Manhattan', 'Astoria')
```

## Top Products

<Tabs fullWidth=true>
    <Tab label="Top 5 products">

        <Tabs>
            <Tab label="Lower Manhattan">
                <BarChart data={top5_LM} x=product_type y=revenue />
            </Tab>
            <Tab label="Astoria">
                <BarChart data={top5_A} x=product_type y=revenue />
            </Tab>
            <Tab label="Hell's Kitchen">
                <BarChart data={top5_HK} x=product_type y=revenue />
            </Tab>
        </Tabs>

    </Tab>
    <Tab label="Top 5 products per month">

        ### Choose Month Here:

        <Dropdown
          data={top5_products_month}
          name=month
          value=month
          label=month
        />

        ## Lower Manhattan

        <BarChart data={top_LM} x=product_type y=revenue_per_month sort=revenue_per_month />

        ## Astoria

        <BarChart data={top_A} x=product_type y=revenue_per_month sort=revenue_per_month  />

        ## Hell's Kitchen

        <BarChart data={top_HK} x=product_type y=revenue_per_month sort=revenue_per_month  />

    </Tab>
    <Tab label="Least 5 sold products">

        <Tabs>
            <Tab label="Lower Manhattan">
                <BarChart data={least5_LM} x=product_type y=least_revenue fillColor="red" />
            </Tab>
            <Tab label="Astoria">
                <BarChart data={least5_A} x=product_type y=least_revenue fillColor="red"  />
            </Tab>
            <Tab label="Hell's Kitchen">
                <BarChart data={least5_HK} x=product_type y=least_revenue fillColor="red"  />
            </Tab>
        </Tabs>

    </Tab>

</Tabs>
