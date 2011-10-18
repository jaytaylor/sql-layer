SELECT customers.name,sku FROM customers
LEFT JOIN orders ON customers.cid = orders.cid
LEFT JOIN items ON orders.oid = items.oid
WHERE name > 'M'
