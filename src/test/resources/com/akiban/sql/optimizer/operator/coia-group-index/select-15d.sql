SELECT * FROM items
 WHERE sku IN (SELECT DISTINCT i2.sku FROM items i2, orders, customers 
                WHERE customers.cid = orders.cid AND orders.oid = i2.oid 
                  AND name = 'Smith')