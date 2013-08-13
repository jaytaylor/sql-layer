SELECT * FROM customers 
 WHERE name IN (SELECT c2.name FROM customers c2, orders 
                 WHERE c2.cid = orders.cid AND order_date < '2011-03-01')