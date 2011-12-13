SELECT name, order_date, city
  FROM customers 
 LEFT JOIN orders ON customers.cid = orders.cid
 LEFT JOIN addresses on customers.cid = addresses.cid AND city <> 'Boston'