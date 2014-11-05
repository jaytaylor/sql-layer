SELECT sku FROM customers INNER JOIN orders USING(cid) INNER JOIN items USING(oid)
 WHERE name = 'Smith' AND sku BETWEEN '1234' AND '5678'