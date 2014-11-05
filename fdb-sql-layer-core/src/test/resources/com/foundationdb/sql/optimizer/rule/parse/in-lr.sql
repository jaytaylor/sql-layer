SELECT * FROM customers INNER JOIN orders USING (cid) INNER JOIN items USING (oid)
 WHERE (name,sku) IN (('Smith','1234'),('Jones','4567'))
