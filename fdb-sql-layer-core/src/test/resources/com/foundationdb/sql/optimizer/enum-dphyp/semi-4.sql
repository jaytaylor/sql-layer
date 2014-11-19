-- This differs from semi-3 in that the conditions are in the ON
-- In Semi-3 it moves the conditions onto the semi-join and nothing gets confused
-- but here, the semi join has no conditions in dphyp
SELECT t1.c1, t1.c2 FROM t1 WHERE EXISTS (
  SELECT 1 FROM t2 JOIN t3
    ON t1.c1 = t2.c1 AND t3.c1 = t2.c2 AND t3.c2 = 7)
  ORDER BY t1.c2
