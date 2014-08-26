SELECT city, sku
  FROM customers LEFT JOIN addresses USING (cid) 
  LEFT JOIN parent USING (name)
  INNER JOIN orders USING (cid) LEFT JOIN items USING (oid) 
 WHERE ORDER_date = '2000-01-01'