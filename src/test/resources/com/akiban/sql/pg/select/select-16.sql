SELECT MIN(order_date), MAX(order_date), COUNT(*)
  FROM orders
  WHERE oid=999
  GROUP BY cid