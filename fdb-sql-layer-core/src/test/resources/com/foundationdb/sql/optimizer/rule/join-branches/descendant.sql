SELECT sku, quan 
  FROM ITEMS INNER JOIN orders ON items.oid = orders.oid
 WHERE order_date > '2011-04-01'