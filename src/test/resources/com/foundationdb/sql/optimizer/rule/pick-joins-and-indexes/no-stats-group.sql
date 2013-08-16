SELECT name, order_date, quan, city
  FROM customers, orders, items, addresses
 WHERE customers.cid = orders.cid
   AND orders.oid = items.oid
   AND customers.cid = addresses.cid
   AND items.sku = '1234'