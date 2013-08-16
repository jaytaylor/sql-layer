SELECT * FROM parent p, child c1, child c2
 WHERE c1.pid = p.id
   AND c2.pid = p.id
