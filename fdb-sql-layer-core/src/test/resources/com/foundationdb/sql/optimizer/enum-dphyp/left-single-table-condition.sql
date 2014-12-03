-- Trying to make sure that the t1.c2 = 3 doesn't end up effectively on the where.
SELECT * FROM t1 LEFT JOIN t2 ON t1.c1 = t2.c2 AND t1.c2 = 3
