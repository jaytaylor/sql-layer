SELECT * FROM customers INNER JOIN orders USING (cid) INNER JOIN items USING (oid)
 WHERE (name,sku) IN (SELECT parent.name, child.name FROM parent INNER JOIN child ON parent.id = child.pid)
