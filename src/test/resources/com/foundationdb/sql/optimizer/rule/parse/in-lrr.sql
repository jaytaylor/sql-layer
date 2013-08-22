SELECT * FROM customers INNER JOIN orders USING (cid) INNER JOIN items USING (oid)
 WHERE (name,(sku,quan)) IN (('Smith',('1234',100)),('Jones',('4567',200)))
