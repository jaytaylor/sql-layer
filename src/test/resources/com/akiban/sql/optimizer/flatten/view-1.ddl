CREATE VIEW names(pname,cname) AS SELECT parent.name,child.name FROM parent,child WHERE parent.id = child.pid
