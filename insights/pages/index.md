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

```sql products
  select 
      *
  from rema.products
  where department_name ilike '${inputs.category.value}'
```


<BarChart
    data={products}
    title="Highest price, {inputs.category.label}"
    x=name
    y=pricing__price
/>

```sql highest_discount_products
  select 
      name,
      pricing__price,
      pricing__normal_price,
      pricing__is_on_discount,
      (1 - (pricing__price/pricing__normal_price)) * 100 as discount
  from ${products}
  where pricing__is_on_discount = true
  order by discount desc
```

<BarChart
    data={highest_discount_products}
    title="Highest discount, {inputs.category.label}"
    x=name
    y=discount
/>