SELECT DATE_FUN(orders.order_date), SUM(price * quan)
  FROM items INNER JOIN orders ON orders.oid = items.oid
 GROUP BY orders.order_date
