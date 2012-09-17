SELECT order_date, sku, quan
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 WHERE (name,sku) IN (('Smith','1234'),('Jones','4567'),('Adams','6666'))
