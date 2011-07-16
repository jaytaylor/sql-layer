SELECT orders.order_date FROM customers INNER JOIN orders ON customers.cid = orders.cid WHERE orders.order_date > '2011-01-01'
