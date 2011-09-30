SELECT COUNT(*)
FROM customers,orders,items
WHERE customers.cid = orders.cid
AND orders.oid = items.oid
AND customers.name = 'Smith'
AND items.sku = '1234'