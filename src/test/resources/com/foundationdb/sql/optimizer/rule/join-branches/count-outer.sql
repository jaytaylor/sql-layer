SELECT COUNT(*) FROM orders LEFT JOIN shipments USING (oid)
 WHERE order_date > '2010-01-01'