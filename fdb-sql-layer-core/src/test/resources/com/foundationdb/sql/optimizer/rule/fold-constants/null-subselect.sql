SELECT name, (SELECT order_date FROM orders
               WHERE 1=0) AS v
 FROM customers
