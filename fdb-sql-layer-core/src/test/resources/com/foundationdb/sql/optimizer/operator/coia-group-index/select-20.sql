SELECT name, (SELECT i2.sku FROM items i1, items i2
               WHERE i1.quan = i2.quan * 10 AND i1.sku = name) 
  FROM customers