SELECT DISTINCT child.name 
  FROM parent INNER JOIN child ON parent.id = child.pid
 WHERE parent.name <> 'Smith'
