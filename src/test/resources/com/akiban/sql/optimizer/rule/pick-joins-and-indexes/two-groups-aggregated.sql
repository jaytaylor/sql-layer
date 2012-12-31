SELECT sku, COUNT(*)
  FROM customers INNER JOIN orders ON customers.cid = orders.cid 
                 INNER JOIN items ON orders.oid = items.oid
                 INNER JOIN parent ON orders.oid = parent.id
 WHERE customers.name = 'Smith'
 GROUP BY sku
 ORDER BY sku
