SELECT customers.cid FROM customers, orders
 WHERE customers.cid = orders.cid
   AND order_date > '2000-01-01'
 GROUP BY customers.cid 
 ORDER BY name 
 LIMIT 10