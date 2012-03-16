SELECT name
  FROM customers, addresses a1, addresses a2, orders
 WHERE customers.cid = a1.cid 
   AND customers.cid = a2.cid 
   AND customers.cid = orders.cid
   AND order_date = '2012-01-01'
   AND a1.city = 'Boston'
   AND a2.city = 'New York'
