UPDATE items SET quan = quan * 2
 WHERE iid IN (SELECT i2.iid FROM items i2 WHERE i2.sku = '1234')