SELECT name FROM child WHERE pid IN (SELECT parent.id FROM parent INNER JOIN child c2 ON parent.id = c2.pid WHERE parent.name = 'foo' AND c2.id = 1)
