SELECT name FROM child WHERE pid > (SELECT parent.id FROM parent WHERE parent.name = 'foo')
