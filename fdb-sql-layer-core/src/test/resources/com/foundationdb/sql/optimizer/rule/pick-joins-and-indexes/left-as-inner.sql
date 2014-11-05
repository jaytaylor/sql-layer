SELECT orders.cid FROM orders INNER JOIN items ON orders.oid = items.oid
 WHERE sku = '1234' AND order_date = '2010-01-01'