SELECT DISTINCT child.name 
  FROM parent LEFT JOIN child ON parent.id = child.pid
