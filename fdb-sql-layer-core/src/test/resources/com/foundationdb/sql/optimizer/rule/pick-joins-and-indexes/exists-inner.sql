-- The key thing that I'm checking here is that the semi
-- stays outside the inner. That was the original bug.
SELECT t1.c1, t1.c2 FROM t1 WHERE EXISTS (
  SELECT 1 FROM t2, t3
    WHERE t1.c1 = t2.c1 AND t3.c1 = t2.c2 AND t3.c2 = 7)
  ORDER BY t1.c2
