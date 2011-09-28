SELECT customers.name,order_date,state
FROM customers,orders,addresses
WHERE customers.cid = orders.cid
 AND customers.cid = addresses.cid
 AND order_date > '2011-01-01'
