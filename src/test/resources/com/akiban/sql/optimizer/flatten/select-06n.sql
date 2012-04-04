SELECT name FROM child WHERE pid IN (SELECT parent.id FROM parent,child c2 WHERE parent.name = 'foo' AND parent.id = c2.pid AND c2.name = 'bar')
