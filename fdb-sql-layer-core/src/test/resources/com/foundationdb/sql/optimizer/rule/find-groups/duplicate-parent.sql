SELECT * FROM parent p1, parent p2, child c1, child c2
 WHERE c1.pid = p1.id
   AND c2.pid = p2.id
