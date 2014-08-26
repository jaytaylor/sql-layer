SELECT order_date, child.name FROM customers 
  LEFT JOIN orders ON customers.cid = orders.cid
  LEFT JOIN parent ON customers.name = parent.name
  LEFT JOIN child ON parent.name = child.name
