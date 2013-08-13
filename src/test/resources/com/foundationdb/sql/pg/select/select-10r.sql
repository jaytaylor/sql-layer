SELECT customers.name,order_date,sku,quan
FROM customers,orders,items
WHERE customers.cid = orders.cid
AND orders.oid = items.oid
AND items.sku = '1234'
ORDER BY order_date DESC