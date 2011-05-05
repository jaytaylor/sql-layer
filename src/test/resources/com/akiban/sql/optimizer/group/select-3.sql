SELECT p1.name,child.name FROM parent AS p1,parent AS p2,child WHERE child.pid = p1.id AND p2.state = 'MA' AND p1.id <> p2.id
