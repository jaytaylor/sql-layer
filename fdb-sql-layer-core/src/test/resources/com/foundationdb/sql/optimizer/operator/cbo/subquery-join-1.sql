SELECT c.name, x.order_date 
  FROM customers c LEFT JOIN (SELECT cid, order_date FROM orders WHERE order_date < '2011-01-01') x ON c.cid = x.cid
