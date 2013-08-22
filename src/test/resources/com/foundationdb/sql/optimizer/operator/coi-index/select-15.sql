SELECT * FROM orders INNER JOIN items ON orders.oid = items.oid
 WHERE orders.order_date = '2000-10-31' 
   AND (items.sku = '1234' OR items.sku = '9876')