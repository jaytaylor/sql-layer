SELECT * FROM parent
 WHERE NOT EXISTS (SELECT * FROM child 
                    WHERE child.pid = parent.id
                      AND child.name = 'Bill')
