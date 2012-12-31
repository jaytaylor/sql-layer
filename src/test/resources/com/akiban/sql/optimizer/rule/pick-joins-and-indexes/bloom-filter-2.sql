SELECT items.sku 
  FROM items, categories
 WHERE items.sku = categories.sku
   AND categories.cat = 2
 ORDER BY items.sku