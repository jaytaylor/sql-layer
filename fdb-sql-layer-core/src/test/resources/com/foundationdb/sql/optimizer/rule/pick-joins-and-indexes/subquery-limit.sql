SELECT c.name FROM customers c WHERE c.cid IN (SELECT cid FROM orders WHERE order_date = '2011-01-01' ORDER BY oid LIMIT 1)
