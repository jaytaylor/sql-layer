SELECT order_date
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 WHERE name = 'Smith'
   AND EXISTS (SELECT * FROM items 
                WHERE orders.oid = items.oid
                  AND sku = '1234')
