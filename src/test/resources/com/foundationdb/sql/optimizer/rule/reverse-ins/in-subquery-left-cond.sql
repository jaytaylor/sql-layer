SELECT name FROM customers
 WHERE cid NOT IN (SELECT cid FROM orders LEFT OUTER JOIN child ON orders.oid = child.pid
                    WHERE child.id IS NULL)