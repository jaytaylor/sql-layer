SELECT COUNT(*) 
  FROM customers c1
 INNER JOIN orders o1 USING (cid)
 INNER JOIN items i1 USING (oid)
 INNER JOIN customers c2 ON c1.cid = c2.cid
 INNER JOIN addresses a2 USING (cid)
 WHERE o1.order_date = '2010-01-01' AND i1.sku = '1234'
   AND c2.name IN ('AAA', 'BBB', 'CCC', 'DDD', 'EEE', 'FFF', 'GGG', 'HHH', 'III')