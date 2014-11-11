SELECT SUM(price * quan)
  FROM items INNER JOIN orders ON orders.oid = items.oid
 GROUP BY orders.oid+1
 ORDER BY orders.oid+1-1
