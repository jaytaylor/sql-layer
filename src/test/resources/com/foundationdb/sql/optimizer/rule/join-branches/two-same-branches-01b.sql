SELECT o2.oid
  FROM customers 
 INNER JOIN orders o1 ON customers.cid = o1.cid
 INNER JOIN orders o2 ON customers.cid = o2.cid
 WHERE o1.order_date = '2011-12-31'
   AND o2.order_date = '2012-01-01'
