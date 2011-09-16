UPDATE items SET quan = 66
  WHERE oid IN (SELECT orders.oid FROM orders WHERE order_date > '2011-04-01')
