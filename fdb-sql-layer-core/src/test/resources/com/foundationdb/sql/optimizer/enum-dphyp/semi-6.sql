-- Make sure we don't move the semi join out
SELECT t1.c1, t1.c2 FROM t1, t2
  WHERE EXISTS (
    SELECT 1 FROM t3
    WHERE t1.c1 = t2.c1 AND t3.c1 = t2.c2 AND t3.c2 = 7)
  ORDER BY t1.c2
