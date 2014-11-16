SELECT customers.name,order_date,addresses.city
FROM customers,orders,addresses
WHERE customers.cid = orders.cid
 AND customers.cid = addresses.cid
 AND addresses.state = 'MA'
