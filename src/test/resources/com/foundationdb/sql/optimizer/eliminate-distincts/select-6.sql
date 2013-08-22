SELECT DISTINCT parent.name 
  FROM parent RIGHT JOIN child ON parent.id = child.pid
