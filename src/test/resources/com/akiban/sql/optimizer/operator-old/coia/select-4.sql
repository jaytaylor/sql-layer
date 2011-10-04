SELECT customers.name,order_date,sku,quan
FROM customers,orders,addresses,items
WHERE customers.cid = orders.cid
 AND orders.oid = items.oid
 AND customers.cid = addresses.cid
 AND addresses.state = 'MA'
