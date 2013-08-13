SELECT child.name FROM child WHERE pid IN (SELECT parent.id FROM parent WHERE state = 'MA')
