SELECT customers.name
  FROM customers LEFT JOIN addresses ON customers.cid = addresses.cid
 WHERE addresses.state IS NULL
   AND customers.name > 'M'