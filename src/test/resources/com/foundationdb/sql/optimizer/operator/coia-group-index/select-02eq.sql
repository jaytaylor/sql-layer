SELECT customers.name,order_date,sku,quan
FROM customers,orders,items
WHERE customers.cid = orders.cid
AND orders.oid = items.oid
AND orders.order_date = '2011-01-01'
AND items.sku = '1234'
ORDER BY order_date