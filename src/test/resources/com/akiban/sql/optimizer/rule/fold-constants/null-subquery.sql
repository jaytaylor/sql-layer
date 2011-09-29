SELECT name, v FROM customers
 LEFT OUTER JOIN (SELECT 'foo' AS n, 123 AS v FROM orders 
                   WHERE 1=0) AS t
  ON name = t.n
