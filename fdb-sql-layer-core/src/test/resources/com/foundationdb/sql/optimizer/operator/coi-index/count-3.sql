SELECT COUNT(*) FROM customers,orders,items WHERE customers.cid = orders.cid AND orders.oid = items.oid AND items.sku < '8888' AND items.quan = 100
