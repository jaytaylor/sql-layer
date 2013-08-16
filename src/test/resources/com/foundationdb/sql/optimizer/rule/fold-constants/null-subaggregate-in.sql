SELECT name
 FROM customers
 WHERE 0 IN (SELECT COUNT(order_date) FROM orders
               WHERE 1=0)