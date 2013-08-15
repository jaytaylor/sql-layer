SELECT x.order_date, x.quan
  FROM (SELECT order_date, sku, quan 
          FROM orders INNER JOIN items ON orders.oid = items.oid
         WHERE order_date < '2011-01-01') x
 WHERE x.sku IN ('1234', '9876')
