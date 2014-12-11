SELECT * FROM (SELECT bit_or(c1) AS bitored FROM t1) AS anon1, (SELECT c1 AS n3, id+3 AS a3 FROM t1) AS anon2
