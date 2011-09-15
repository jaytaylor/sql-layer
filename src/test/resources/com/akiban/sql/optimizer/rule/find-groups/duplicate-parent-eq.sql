SELECT * FROM parent p1, parent p2, child c
 WHERE c.pid = p1.id
   AND c.pid = p2.id
