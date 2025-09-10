---
title: Rema 1000 oversigt
---
```sql departments
  select
      name
  from rema.departments
  group by 1;
```

```sql products
  select 
      'https://shop.rema1000.dk/varer/' || id::integer as link,
      if(hf2 = 'REMA 1000', true, false) as is_rema_product,
      *
  from rema.products
  where department_name ilike '${inputs.category.value}'
```

<Dropdown data={departments} name=category value=name>
    <DropdownOption value="%" valueLabel="Alle Kategorier"/>
</Dropdown>


```sql number_of_products
  select 
      count(*) as products,
      count(distinct department_id) as categories,
      max((1 - (pricing__price/pricing__normal_price)) * 100) as max_discount,
      count(if(pricing__is_on_discount = true, 1, null)) as products_on_discount,
      count(if(is_rema_product = true, 1, null)) as rema_products
  from ${products}
```


```sql max_price
  select 
      pricing__price as max_price,
      name
  from ${products}
  qualify pricing__price = max(pricing__price) over ()
```

```sql min_price
  select 
      pricing__price as min_price,
      name
  from ${products}
  qualify pricing__price = min(pricing__price) over ()
```

```sql median_price
  select 
      approx_quantile(pricing__price, 0.5) as median_price
  from ${products}
```

<center>
<Grid cols=4>
<BigValue
    title="Antal produkter"
    data={number_of_products}
    value="products"
    comparison="rema_products"
    comparisonDelta=false
    comparisonTitle="rema produkter"
/>
<BigValue
    title="Højeste rabat %"
    data={number_of_products}
    value=max_discount
    comparison=products_on_discount
    comparisonDelta=false
    comparisonTitle="tilbud"
/>
<BigValue
    title="Højeste pris"
    data={max_price}
    value=max_price
    comparison=name
    comparisonDelta=false
    comparisonTitle=""
/>
<BigValue
    title="Laveste pris"
    data={min_price}
    value=min_price
    comparison=name
    comparisonDelta=false
    comparisonTitle=""
/>
</Grid>
</center>

<BarChart
    data={products}
    title="Højeste pris, {inputs.category.label}"
    x=name
    y=pricing__price
/>

#### **Alle produkter i {inputs.category.label}**
<DataTable data={products} search=true>
  <Column id="link" title='Navn' contentType=link openInNewTab=true linkLabel='Gå til shop ->' />
  <Column id="department_name" title="Afdeling" />
  <Column id="category_name" title="Kategori" />
  <Column id="name" title="Produktnavn"/>
  <Column id="underline" />
  <Column id="pricing__price" title="Pris" />
  <Column id="pricing__normal_price" title="Normal pris" />
  <Column id="pricing__max_quantity" title="Maks antal med rabat" />
  <Column id="pricing__price_over_max" title="Pris over maks antal" />
  <Column id="pricing__is_on_discount" title="På tilbud" contentType=boolean />
  <Column id="pricing__price_per_kilogram" title="Pris per Kg" />
  <Column id="pricing__price_per_unit" title="Pris per enhed" />
  <Column id="pricing__is_advertised" title="Annonceret" contentType=boolean />
  <Column id="pricing__deposit" title="Pant" />
  <Column id="pricing__deposit__v_double" title="Pant decimal" />
  <Column id="extra__popularity" title="Popularitet" />
  <Column id="entering_date" title="Dato" />
  <Column id="pricing__price_changes_on" title="Pris ændres dato" />
  <Column id="pricing__price_changes_type" title="Pris ændringstype" />
  <Column id="assortment_label" title="Sortiment label" />
  <Column id="max_sales_quantity" title="Maks salgsantal" />
</DataTable>


```sql highest_discount_products
  select
      link,
      name,
      underline,
      pricing__price,
      pricing__normal_price,
      pricing__is_on_discount,
      (1 - (pricing__price/pricing__normal_price)) as discount_pct
  from ${products}
  where pricing__is_on_discount = true
  order by discount_pct desc
```

<BarChart
    data={highest_discount_products}
    title="Højeste rabat på tilbud, {inputs.category.label}"
    x=name
    y=discount_pct
    yFmt="pct"
/>

<DataTable data={highest_discount_products} search=true>
  <Column id="link" title='Link' contentType=link openInNewTab=true linkLabel='Gå til shop ->' />
  <Column id="name" title="Produktnavn" />
  <Column id="underline" />
  <Column id="pricing__price" title="Pris" />
  <Column id="pricing__normal_price" title="Normal Pris" />
  <Column id="pricing__is_on_discount" title="På tilbud" contentType=boolean />
  <Column id="discount_pct" title="Rabat %" />
</DataTable>
