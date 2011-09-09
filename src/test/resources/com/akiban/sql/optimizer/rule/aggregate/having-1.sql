SELECT orders.order_date, SUM(price * quan)
  FROM items INNER JOIN orders ON orders.oid = items.oid
 GROUP BY orders.order_date
 HAVING SUM(price * quan) > 1000.00


