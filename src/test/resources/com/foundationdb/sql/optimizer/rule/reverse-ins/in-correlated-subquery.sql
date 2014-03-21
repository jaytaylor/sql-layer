SELECT name FROM child
 WHERE pid IN (SELECT cid FROM orders WHERE oid <> child.id)
