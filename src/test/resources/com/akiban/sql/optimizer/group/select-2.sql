SELECT parent.name,child.name FROM parent,child WHERE child.pid = parent.id AND parent.state = 'MA'
