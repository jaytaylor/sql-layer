SELECT orders.order_date, price, SUM(quan)
  FROM items INNER JOIN orders ON orders.oid = items.oid
 GROUP BY orders.order_date
