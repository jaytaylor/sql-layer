SELECT x.* FROM (SELECT c.name, COUNT(*) FROM customers c, addresses a
                  WHERE c.cid = a.cid AND a.state = 'MA'
                  GROUP BY 1) x
 WHERE x.name = 'Smith'