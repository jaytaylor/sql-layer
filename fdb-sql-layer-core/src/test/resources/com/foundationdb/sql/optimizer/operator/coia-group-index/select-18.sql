SELECT sku, name
FROM customers JOIN orders USING (cid) LEFT JOIN items USING (oid)
WHERE name = 'Smith'
AND (sku != '1111' AND sku != '2222' AND sku != '3333')