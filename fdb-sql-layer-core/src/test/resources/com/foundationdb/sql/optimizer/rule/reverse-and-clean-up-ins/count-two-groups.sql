SELECT COUNT(*)
  FROM customers
 INNER JOIN orders ON customers.cid = orders.cid
 INNER JOIN items ON orders.oid = items.oid
 CROSS JOIN parent
 INNER JOIN child ON parent.id = child.pid
 WHERE customers.name = parent.name
   AND sku IN ('1234', '4567', '6666')
