SELECT orders.order_date, AVG(quan)
  FROM items INNER JOIN orders ON orders.oid = items.oid
 WHERE items.sku = ?
 GROUP BY orders.order_date
