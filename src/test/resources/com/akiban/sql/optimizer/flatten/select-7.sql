SELECT name FROM parent p WHERE EXISTS (SELECT * from child c WHERE c.pid = p.id AND c.id = 3)
