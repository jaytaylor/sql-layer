SELECT customers.name, items.quan
  FROM customers, orders, items, addresses
 WHERE customers.cid = orders.cid
   AND orders.oid = items.oid
   AND customers.cid = addresses.cid
   AND addresses.state IN ('MA', 'NY', 'VT')
 ORDER BY quan DESC
