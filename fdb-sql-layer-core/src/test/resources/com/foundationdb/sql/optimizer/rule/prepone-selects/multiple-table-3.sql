SELECT customers.name
  FROM customers, addresses
 WHERE customers.name > 'M' AND addresses.state > 'M'
   AND customers.name <> addresses.state