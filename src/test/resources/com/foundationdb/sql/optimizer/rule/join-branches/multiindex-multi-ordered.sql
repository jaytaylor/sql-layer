SELECT i1.quan, i2.quan
  FROM customers, orders o1, orders o2, items i1, items i2
 WHERE customers.cid = o1.cid AND customers.cid = o2.cid AND o1.oid = i1.oid AND o2.oid = i2.oid
   AND i1.sku = '1234' AND i2.sku = '4567'
 ORDER BY customers.cid DESC