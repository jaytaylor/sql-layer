SELECT customers.name,order_date,state
FROM customers,orders,addresses
WHERE customers.cid = orders.cid
 AND customers.cid = addresses.cid
 AND customers.name = 'IBM'
