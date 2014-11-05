SELECT * FROM
 (parent LEFT JOIN child ON parent.id = child.pid)
INNER JOIN
 (customers LEFT JOIN orders ON customers.cid = orders.cid
            LEFT JOIN items ON orders.oid = items.oid)
 ON parent.name = customers.name AND child.name = items.sku
