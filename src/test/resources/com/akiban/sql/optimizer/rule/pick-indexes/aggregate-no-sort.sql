SELECT MIN(sku), MAX(sku) FROM items
 WHERE oid = ?
 GROUP BY quan
