SELECT name FROM child WHERE pid IN (SELECT parent.id FROM parent,child WHERE parent.name = 'foo')
