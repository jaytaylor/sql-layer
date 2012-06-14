SELECT city, addresses.state, child.name
  FROM customers
 INNER JOIN addresses ON customers.cid = addresses.cid
 CROSS JOIN parent
 INNER JOIN child ON parent.id = child.pid
 WHERE customers.name = parent.name
   AND addresses.state IN ('MA', 'NH', 'VT')
