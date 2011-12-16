SELECT * FROM customers c1
 INNER JOIN customers c2 ON c1.name = c2.name AND c1.cid <> c2.cid
 INNER JOIN child ON c2.name = child.name
  LEFT JOIN parent ON c1.name = parent.name
