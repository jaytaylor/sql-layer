-- NOTE: if we fix ANTI join to correctly handle null values this can be switched back to an anti join
-- instead of a subquery.
SELECT name FROM customers
 WHERE cid NOT IN (SELECT cid FROM orders LEFT OUTER JOIN child ON orders.oid = child.pid
                    WHERE child.id IS NULL)