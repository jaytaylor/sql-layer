SELECT * FROM customers INNER JOIN parent ON customers.cid = parent.id
 WHERE FULL_TEXT_SEARCH(customers.name, 'john') 
   AND parent.name = 'X'