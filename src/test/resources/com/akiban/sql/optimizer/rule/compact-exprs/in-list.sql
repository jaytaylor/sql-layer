SELECT order_date, sku, quan
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 WHERE name = 'Smith'
    OR sku in ('1234', '9876', '6666')
