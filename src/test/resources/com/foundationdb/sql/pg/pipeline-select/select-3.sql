SELECT c.name, a.city, a.state, o1.order_date, i1.quan, o2.order_date, i2.sku, i2.quan
  FROM customers c
 INNER JOIN addresses a ON c.cid = a.cid
 INNER JOIN orders o1 ON c.cid = o1.cid INNER JOIN items i1 ON o1.oid = i1.oid
 INNER JOIN orders o2 ON c.cid = o2.cid INNER JOIN items i2 ON o2.oid = i2.oid
 WHERE i1.sku = '1234' AND i1.iid <> i2.iid