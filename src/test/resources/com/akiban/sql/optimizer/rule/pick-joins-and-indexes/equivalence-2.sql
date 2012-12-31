SELECT city, COUNT(*)
  FROM customers, addresses
 WHERE customers.cid = addresses.cid
   AND city = name
   AND state = 'NY'
 GROUP BY 1
