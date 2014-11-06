SELECT customers.name
  FROM customers, addresses, parent
 WHERE customers.name LIKE 'M%' AND addresses.state > 'M'
   AND parent.name = customers.name
   AND customers.name <> addresses.state AND addresses.state <> 'WY'
   AND parent.name <> addresses.city