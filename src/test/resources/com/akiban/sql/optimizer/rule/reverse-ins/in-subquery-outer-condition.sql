SELECT order_date, sku, quan
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 WHERE name = 'Smith'
   AND quan IN (SELECT i2.quan+items.quan FROM items i2
                 WHERE i2.sku = '1234')
