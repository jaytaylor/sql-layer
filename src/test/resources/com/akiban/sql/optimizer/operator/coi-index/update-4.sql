UPDATE items SET quan = quan + 2
WHERE oid IN (SELECT orders.oid FROM orders WHERE order_date > '2011-04-01')
RETURNING iid, quan