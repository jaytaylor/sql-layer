SELECT sku||state FROM items, addresses
 WHERE addresses.cid IN (SELECT orders.cid FROM orders WHERE orders.oid = items.oid)
