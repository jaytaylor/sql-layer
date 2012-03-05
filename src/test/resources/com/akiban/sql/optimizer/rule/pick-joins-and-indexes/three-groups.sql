SELECT * FROM customers
  JOIN orders ON (customers.cid = orders.cid)
  JOIN child ON (orders.oid = child.pid)
  JOIN items ON (items.oid = child.id)