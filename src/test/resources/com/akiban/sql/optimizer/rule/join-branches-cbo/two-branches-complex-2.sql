SELECT name, order_date, city
  FROM customers 
 LEFT JOIN addresses on customers.cid = addresses.cid
 LEFT JOIN orders ON customers.cid = orders.cid AND orders.order_date <> '2011-01-01'