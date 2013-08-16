SELECT parent.id, customers.cid FROM customers 
 INNER JOIN parent USING (name)
 INNER JOIN addresses USING (cid)
 WHERE addresses.state = 'MA'
   AND parent.name > 'M'