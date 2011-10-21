SELECT * FROM items
 WHERE sku IN (SELECT DISTINCT i2.sku FROM orders
                INNER JOIN items i2 ON orders.oid = i2.oid 
                WHERE order_date > '2011-01-01')