SELECT customers.name,order_date,city
FROM customers,orders,addresses
WHERE customers.cid = orders.cid
 AND customers.cid = addresses.cid
 AND addresses.state = 'MA'
