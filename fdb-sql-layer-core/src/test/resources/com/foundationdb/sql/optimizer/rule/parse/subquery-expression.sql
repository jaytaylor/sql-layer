SELECT order_date, (SELECT max(price) FROM items WHERE items.oid = orders.oid) 
  FROM orders
