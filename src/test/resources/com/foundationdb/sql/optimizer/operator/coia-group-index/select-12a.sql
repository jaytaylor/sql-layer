SELECT sku FROM customers INNER JOIN orders USING(cid) INNER JOIN items USING(oid)
 WHERE name = 'Smith' AND sku >= '1234'