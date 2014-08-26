SELECT order_date FROM orders
 WHERE EXISTS (SELECT * FROM items, child
                WHERE items.oid = orders.oid 
                  AND child.id = items.iid
                  AND sku = '1234'
                  AND child.name <> 'Astro')
           OR order_date = '0000-00-00'