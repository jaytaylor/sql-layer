SELECT customers.cid
  FROM customers
 INNER JOIN orders o1 ON customers.cid = o1.cid
 INNER JOIN orders o2 ON customers.cid = o2.cid
 INNER JOIN orders o3 ON customers.cid = o3.cid
 WHERE o1.order_date = o2.order_date AND o1.oid > o2.oid
   AND o2.order_date = o3.order_date AND o2.oid > o3.oid
   AND weekday(o2.order_date) = 3 AND weekday(o3.order_date) = 2