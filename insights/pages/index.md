---
title: Rema 1000 oversigt
---


```sql departments
  select
      name
  from rema.departments
  group by 1;
```

<Dropdown data={departments} name=category value=name>
    <DropdownOption value="%" valueLabel="All Categories"/>
</Dropdown>

```sql orders_by_category
  select 
      date_trunc('month', order_datetime) as month,
      sum(sales) as sales_usd,
      category
  from needful_things.orders
  where category like '${inputs.category.value}'
  and date_part('year', order_datetime) like '${inputs.year.value}'
  group by all
  order by sales_usd desc
```

<BarChart
    data={orders_by_category}
    title="Sales by Month, {inputs.category.label}"
    x=month
    y=sales_usd
    series=category
/>

