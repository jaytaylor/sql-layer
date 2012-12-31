SELECT * FROM orders INNER JOIN items ON orders.oid = items.oid
 WHERE orders.order_date = '2000-10-31' 
   AND (items.iid = 100 OR items.iid = 200 OR items.iid = 1000)