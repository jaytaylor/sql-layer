SELECT name, order_date
  FROM customers RIGHT JOIN orders ON customers.cid = orders.cid
 WHERE name IS NULL
