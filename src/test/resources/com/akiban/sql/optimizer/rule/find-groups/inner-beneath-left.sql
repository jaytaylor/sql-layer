SELECT * FROM customers
 INNER JOIN child ON customers.name = child.name
 INNER JOIN orders ON customers.cid = orders.cid
  LEFT JOIN parent ON child.pid = parent.id
