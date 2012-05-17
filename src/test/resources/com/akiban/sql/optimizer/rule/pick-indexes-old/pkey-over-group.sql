SELECT customers.name FROM customers
  LEFT JOIN parent ON customers.name = parent.name
  LEFT JOIN orders ON customers.cid = orders.cid AND orders.oid = parent.id