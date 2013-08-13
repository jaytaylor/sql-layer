SELECT * FROM orders INNER JOIN items ON orders.oid = items.oid
 WHERE (orders.order_date = '2000-10-31' OR orders.order_date = '2000-11-01')
   AND (items.iid = 100 OR items.iid = 200)