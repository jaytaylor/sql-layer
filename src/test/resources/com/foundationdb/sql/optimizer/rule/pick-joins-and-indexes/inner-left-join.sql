-- this was inspired by test-right-join which was caused by Sequel gem schema parsing
SELECT t3.c1,t3.c2,t1.c3
FROM t1
INNER JOIN t2
  ON (t1.c3 = 333)
  AND (t1.c1 = t2.c2)
  AND (t1.c2 = t2.c2)
LEFT OUTER JOIN t3
  USING (c1, c2)
WHERE t3.c1 = 111
  AND t3.c2 = 222

