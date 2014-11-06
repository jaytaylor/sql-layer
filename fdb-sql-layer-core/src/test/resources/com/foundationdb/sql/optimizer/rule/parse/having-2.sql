SELECT orders.order_date, SUM(price * quan) AS total
  FROM items INNER JOIN orders ON orders.oid = items.oid
 GROUP BY orders.order_date
 HAVING total > 1000.00


