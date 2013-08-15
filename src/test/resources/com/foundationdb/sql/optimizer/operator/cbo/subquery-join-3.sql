SELECT COUNT(*) FROM customers
 INNER JOIN (SELECT CAST(i1.iid + 1 AS INTEGER) AS v0, CAST(i2.iid + 1 AS INTEGER) AS v1 FROM items i1 INNER JOIN items i2 ON i1.quan = i2.quan) AS v ON v.v0 = cid
  LEFT JOIN orders ON v.v1 = oid