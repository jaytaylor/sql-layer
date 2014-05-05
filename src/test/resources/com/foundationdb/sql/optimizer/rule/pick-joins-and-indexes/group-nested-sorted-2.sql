SELECT customers.name, (SELECT orders.order_date FROM orders
                               WHERE customers.cid = orders.cid)
  FROM customers
 ORDER BY customers.cid * 2
