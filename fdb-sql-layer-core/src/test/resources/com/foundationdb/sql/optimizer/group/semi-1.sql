-- I know none of these are grouped, but
SELECT t1.c1, t1.c2 FROM t1 WHERE EXISTS (
  SELECT 1 FROM t2, t3
    WHERE t1.c1 = t2.c1 AND t3.c1 = t2.c3 AND t3.c2 = 'n3')
  ORDER BY t1.c2;
