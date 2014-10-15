UPDATE items SET price = price * .95, sku = sku||'BULK', quan = quan / 1000
 WHERE quan > 1000 AND sku = ?
   AND oid IN (SELECT orders.oid FROM orders WHERE order_date > '2011-01-01')
