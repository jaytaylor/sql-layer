SELECT name, city
  FROM addresses LEFT JOIN customers ON customers.cid = addresses.cid
 WHERE state = 'MA'
 ORDER BY name
