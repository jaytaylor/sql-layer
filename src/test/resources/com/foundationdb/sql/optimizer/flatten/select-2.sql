SELECT name FROM child WHERE pid = ANY (SELECT parent.id FROM parent WHERE parent.name = 'foo')
