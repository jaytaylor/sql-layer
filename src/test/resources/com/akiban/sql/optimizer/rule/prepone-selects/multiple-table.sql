SELECT customers.cid
  FROM customers
 INNER JOIN orders o1 ON customers.cid = o1.cid
 INNER JOIN orders o2 ON customers.cid = o2.cid
 WHERE o1.order_date = o2.order_date AND o1.oid > o2.oid
