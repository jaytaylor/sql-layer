SELECT name FROM customers
 WHERE cid NOT IN (SELECT cid FROM orders
                    WHERE oid NOT IN (SELECT oid FROM items WHERE sku = '1234'))