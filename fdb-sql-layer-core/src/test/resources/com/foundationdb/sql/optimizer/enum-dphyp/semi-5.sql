-- Make sure we don't move the semi join out
-- Technically:
-- t2 INNER JOIN (t1 SEMI JOIN t3 ON t3.c1 == t2.c2 AND t3.c2 == 7) ON t1.c1 == t2.c1
-- should also work, but that's an optimization issue, and right now I'm focusing on
-- correctness
SELECT t1.c1, t1.c2 FROM t1 JOIN t2 ON t1.c1 = t2.c1
  WHERE EXISTS (
    SELECT 1 FROM t3
    WHERE t3.c1 = t2.c2 AND t3.c2 = 7)
  ORDER BY t1.c2
