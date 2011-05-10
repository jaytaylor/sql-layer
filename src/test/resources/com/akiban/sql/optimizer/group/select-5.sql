SELECT parent.name,child.name FROM parent INNER JOIN child ON child.pid = parent.id WHERE parent.state = 'MA'
