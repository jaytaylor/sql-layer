-- Found with the hibernate tests, when we combined a left join with a cross join, it would throw a
-- NullPointerException when evaluating a join that had no conditions
SELECT * FROM t1
LEFT OUTER JOIN t2
ON t1.c1 = t2.c1,
t3
WHERE t1.c2 = t3.c2