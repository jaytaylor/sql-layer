SELECT items.sku FROM items
 WHERE EXISTS (SELECT * FROM categories 
                WHERE items.sku = categories.sku 
                  AND categories.cat = 1)
 ORDER BY items.sku