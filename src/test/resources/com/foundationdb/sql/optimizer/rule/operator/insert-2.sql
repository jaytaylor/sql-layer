INSERT INTO child(pid, name) SELECT id, name FROM parent WHERE name <> 'IGNORE'
