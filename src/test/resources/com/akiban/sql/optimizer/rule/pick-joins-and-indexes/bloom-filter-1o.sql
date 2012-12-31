SELECT items.sku 
  FROM items LEFT JOIN categories ON items.sku = categories.sku AND categories.cat = 1
 ORDER BY items.sku