SELECT customers.name,order_date,sku,state
FROM customers,orders,items,addresses
WHERE customers.cid = orders.cid
 AND orders.oid = items.oid
 AND customers.cid = addresses.cid
 AND order_date > '2011-01-01'
