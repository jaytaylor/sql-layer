SELECT name
  FROM customers, addresses, orders o1, items i1, orders o2, items i2
 WHERE customers.cid = addresses.cid
   AND customers.cid = o1.cid
   AND o1.oid = i1.oid
   AND customers.cid = o2.cid
   AND o2.oid = i2.oid
   AND state = 'MA'
   AND o1.oid > o2.oid
   AND i1.quan > 100
   AND i2.quan > 100
