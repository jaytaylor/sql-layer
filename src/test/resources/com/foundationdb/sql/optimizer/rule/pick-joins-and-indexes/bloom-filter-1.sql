SELECT items.sku 
  FROM items, categories
 WHERE items.sku = categories.sku
   AND categories.cat = 1
 ORDER BY items.sku